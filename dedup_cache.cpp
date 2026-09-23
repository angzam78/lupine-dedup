#include "dedup_cache.h"

#include <atomic>
#include <climits>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <lz4.h>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

#include "lupine_log.h"
#include "lupine_platform.h"

namespace {

namespace fs = std::filesystem;

struct cache_header {
  char magic[8];
  uint32_t version;
  uint32_t reserved;
  uint64_t hash_low;
  uint64_t hash_high;
  uint64_t bytes;
  uint64_t payload_bytes;
  uint8_t flags;
  uint8_t reserved_bytes[7];
};

constexpr char kMagic[] = "LUPDEDUP";
constexpr uint32_t kVersion = 2;
constexpr uint8_t kFlagLz4 = 1;

struct cache_job {
  lupine_dedup_key key;
  std::vector<unsigned char> data;
  bool lz4_compressed = false;
};

struct cache_profile {
  std::atomic<uint64_t> read_calls{0};
  std::atomic<uint64_t> read_bytes{0};
  std::atomic<uint64_t> read_ns{0};
  std::atomic<uint64_t> file_read_ns{0};
  std::atomic<uint64_t> decompress_calls{0};
  std::atomic<uint64_t> decompress_ns{0};
  std::atomic<uint64_t> hash_calls{0};
  std::atomic<uint64_t> hash_bytes{0};
  std::atomic<uint64_t> hash_ns{0};
  std::atomic<uint64_t> mtime_calls{0};
  std::atomic<uint64_t> mtime_ns{0};
  std::atomic_flag reporting = ATOMIC_FLAG_INIT;
};

cache_profile &profile() {
  static auto *value = new cache_profile;
  return *value;
}

bool profiling_enabled() {
  static const bool enabled = [] {
    const char *value = std::getenv("LUPINE_DEDUP_PROFILE");
    return value != nullptr && value[0] != '\0' && std::strcmp(value, "0") != 0;
  }();
  return enabled;
}

void maybe_report_profile() {
  if (!profiling_enabled()) {
    return;
  }
  cache_profile &value = profile();
  uint64_t reads = value.read_calls.load(std::memory_order_relaxed);
  if (reads == 0 || reads % 256 != 0 ||
      value.reporting.test_and_set(std::memory_order_acquire)) {
    return;
  }
  uint64_t read_bytes = value.read_bytes.load(std::memory_order_relaxed);
  uint64_t read_ns = value.read_ns.load(std::memory_order_relaxed);
  uint64_t file_read_ns =
      value.file_read_ns.load(std::memory_order_relaxed);
  uint64_t decompress_calls =
      value.decompress_calls.load(std::memory_order_relaxed);
  uint64_t decompress_ns =
      value.decompress_ns.load(std::memory_order_relaxed);
  uint64_t hash_calls = value.hash_calls.load(std::memory_order_relaxed);
  uint64_t hash_bytes = value.hash_bytes.load(std::memory_order_relaxed);
  uint64_t hash_ns = value.hash_ns.load(std::memory_order_relaxed);
  uint64_t mtime_calls = value.mtime_calls.load(std::memory_order_relaxed);
  uint64_t mtime_ns = value.mtime_ns.load(std::memory_order_relaxed);
  LUPINE_LOG_DEBUG("Dedup profile reads=" << reads << " read_bytes="
                   << read_bytes << " read_ms=" << read_ns / 1000000.0
                   << " file_ms=" << file_read_ns / 1000000.0
                   << " decompress_calls=" << decompress_calls
                   << " decompress_ms=" << decompress_ns / 1000000.0
                   << " hash_calls=" << hash_calls << " hash_bytes="
                   << hash_bytes << " hash_ms=" << hash_ns / 1000000.0
                   << " mtime_calls=" << mtime_calls << " mtime_ms="
                   << mtime_ns / 1000000.0);
  value.reporting.clear(std::memory_order_release);
}

class profile_read_scope {
public:
  explicit profile_read_scope(uint64_t bytes)
      : enabled_(profiling_enabled()), bytes_(bytes),
        start_(std::chrono::steady_clock::now()) {}

  ~profile_read_scope() {
    if (!enabled_) {
      return;
    }
    cache_profile &value = profile();
    value.read_calls.fetch_add(1, std::memory_order_relaxed);
    value.read_bytes.fetch_add(bytes_, std::memory_order_relaxed);
    value.read_ns.fetch_add(
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now() - start_)
                                   .count()),
        std::memory_order_relaxed);
    maybe_report_profile();
  }

private:
  bool enabled_;
  uint64_t bytes_;
  std::chrono::steady_clock::time_point start_;
};

void record_mtime(std::chrono::steady_clock::time_point start) {
  if (!profiling_enabled()) {
    return;
  }
  cache_profile &value = profile();
  value.mtime_calls.fetch_add(1, std::memory_order_relaxed);
  value.mtime_ns.fetch_add(
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count()),
      std::memory_order_relaxed);
}

constexpr char kDefaultCacheDirectory[] = "/var/cache/lupine/dedup";
constexpr char kDefaultCacheSize[] = "32GiB";
constexpr size_t kDefaultQueuedCacheBytes =
    static_cast<size_t>(1024ull * 1024ull * 1024ull);

struct cache_state {
  std::once_flag once;
  bool enabled = false;
  bool verify_entries = false;
  uint64_t limit = 0;
  size_t queue_limit = kDefaultQueuedCacheBytes;
  fs::path directory;
  std::mutex queue_mutex;
  std::condition_variable queue_condition;
  std::deque<cache_job> queue;
  size_t queued_bytes = 0;
  bool worker_busy = false;
  bool worker_started = false;
};

cache_state &state() {
  static auto *value = new cache_state;
  return *value;
}

void cache_worker();

void initialize() {
  cache_state &value = state();
  const char *directory = std::getenv("LUPINE_DEDUP_CACHE_DIR");
  const char *size = std::getenv("LUPINE_DEDUP_CACHE_SIZE");
  const char *queue_size = std::getenv("LUPINE_DEDUP_CACHE_QUEUE_SIZE");
  if (directory == nullptr || directory[0] == '\0') {
    directory = kDefaultCacheDirectory;
  }
  if (size == nullptr || size[0] == '\0') {
    size = kDefaultCacheSize;
  }
  uint64_t limit = 0;
  if (!lupine_parse_size(size, &limit) || limit == 0) {
    return;
  }

  size_t queue_limit = kDefaultQueuedCacheBytes;
  if (queue_size != nullptr) {
    uint64_t parsed_queue_limit = 0;
    if (lupine_parse_size(queue_size, &parsed_queue_limit) &&
        parsed_queue_limit > 0 &&
        parsed_queue_limit <= std::numeric_limits<size_t>::max()) {
      queue_limit = static_cast<size_t>(parsed_queue_limit);
    } else {
      LUPINE_LOG_ERROR("Invalid LUPINE_DEDUP_CACHE_QUEUE_SIZE: "
                       << queue_size);
    }
  }

  bool verify_entries = false;
  const char *verify = std::getenv("LUPINE_DEDUP_CACHE_VERIFY");
  if (verify != nullptr && verify[0] != '\0') {
    if (std::strcmp(verify, "0") == 0) {
      verify_entries = false;
    } else if (std::strcmp(verify, "1") != 0) {
      LUPINE_LOG_ERROR("Invalid LUPINE_DEDUP_CACHE_VERIFY: " << verify
                       << "; using 1");
    }
  }

  std::error_code error;
  fs::create_directories(directory, error);
  if (error) {
    LUPINE_LOG_ERROR("Unable to create deduplication cache directory "
                     << directory << ": " << error.message());
    return;
  }
  value.directory = directory;
  value.limit = limit;
  value.queue_limit = queue_limit;
  value.verify_entries = verify_entries;
  value.enabled = true;
  LUPINE_LOG_DEBUG("Dedup cache queue limit: " << value.queue_limit
                   << " bytes; verify=" << (value.verify_entries ? 1 : 0));
  {
    std::lock_guard<std::mutex> lock(value.queue_mutex);
    value.worker_started = true;
  }
  std::thread(cache_worker).detach();
}

bool enabled() {
  cache_state &value = state();
  std::call_once(value.once, initialize);
  return value.enabled;
}

std::string hex(uint64_t value) {
  std::ostringstream output;
  output << std::hex << std::setfill('0') << std::setw(16) << value;
  return output.str();
}

fs::path entry_path(const lupine_dedup_key &key) {
  fs::path directory = state().directory / "v1" / hex(key.hash_high).substr(0, 2);
  return directory / (hex(key.hash_high) + "-" + hex(key.hash_low) + "-" +
                      std::to_string(key.bytes) + ".chunk");
}

#ifndef _WIN32
class cache_lock {
public:
  explicit cache_lock(const fs::path &directory) {
    fd_ = open((directory / ".lock").c_str(), O_CREAT | O_RDWR, 0600);
    if (fd_ >= 0) {
      locked_ = flock(fd_, LOCK_EX) == 0;
    }
  }

  ~cache_lock() {
    if (locked_) {
      (void)flock(fd_, LOCK_UN);
    }
    if (fd_ >= 0) {
      close(fd_);
    }
  }

  bool acquired() const { return locked_; }

private:
  int fd_ = -1;
  bool locked_ = false;
};
#else
class cache_lock {
public:
  explicit cache_lock(const fs::path &) {}
  bool acquired() const { return true; }
};
#endif

bool read_entry(const fs::path &path, const lupine_dedup_key &key,
                std::vector<unsigned char> *data) {
  profile_read_scope read_profile(key.bytes);
  auto file_start = std::chrono::steady_clock::now();
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return false;
  }
  cache_header header = {};
  input.read(reinterpret_cast<char *>(&header), sizeof(header));
  if (!input || std::memcmp(header.magic, kMagic, sizeof(header.magic)) != 0 ||
      header.version != kVersion || header.hash_low != key.hash_low ||
      header.hash_high != key.hash_high || header.bytes != key.bytes ||
      (header.flags & ~kFlagLz4) != 0 || header.payload_bytes == 0 ||
      key.bytes > static_cast<uint64_t>(SIZE_MAX) ||
      header.payload_bytes > static_cast<uint64_t>(SIZE_MAX) ||
      header.payload_bytes >
          static_cast<uint64_t>(std::numeric_limits<std::streamsize>::max())) {
    return false;
  }
  std::error_code error;
  uintmax_t file_size = fs::file_size(path, error);
  if (error || file_size != sizeof(cache_header) + header.payload_bytes) {
    return false;
  }
  std::vector<unsigned char> encoded(static_cast<size_t>(header.payload_bytes));
  input.read(reinterpret_cast<char *>(encoded.data()),
             static_cast<std::streamsize>(encoded.size()));
  if (!input) {
    return false;
  }
  if (profiling_enabled()) {
    profile().file_read_ns.fetch_add(
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now() - file_start)
                                   .count()),
        std::memory_order_relaxed);
  }
  data->resize(static_cast<size_t>(key.bytes));
  if ((header.flags & kFlagLz4) != 0) {
    if (key.bytes > static_cast<uint64_t>(INT_MAX) ||
        encoded.size() > static_cast<size_t>(INT_MAX)) {
      data->clear();
      return false;
    }
    auto decompress_start = std::chrono::steady_clock::now();
    int decompressed = LZ4_decompress_safe(
        reinterpret_cast<const char *>(encoded.data()),
        reinterpret_cast<char *>(data->data()), static_cast<int>(encoded.size()),
        static_cast<int>(key.bytes));
    if (profiling_enabled()) {
      cache_profile &metrics = profile();
      metrics.decompress_calls.fetch_add(1, std::memory_order_relaxed);
      metrics.decompress_ns.fetch_add(
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now() -
                                     decompress_start)
                                     .count()),
          std::memory_order_relaxed);
    }
    if (decompressed != static_cast<int>(key.bytes)) {
      data->clear();
      return false;
    }
  } else {
    if (encoded.size() != data->size()) {
      data->clear();
      return false;
    }
    std::memcpy(data->data(), encoded.data(), encoded.size());
  }
  if (state().verify_entries) {
    auto hash_start = std::chrono::steady_clock::now();
    lupine_dedup_key actual = lupine_dedup_hash(data->data(), data->size());
    if (profiling_enabled()) {
      cache_profile &metrics = profile();
      metrics.hash_calls.fetch_add(1, std::memory_order_relaxed);
      metrics.hash_bytes.fetch_add(data->size(), std::memory_order_relaxed);
      metrics.hash_ns.fetch_add(
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now() - hash_start)
                                     .count()),
          std::memory_order_relaxed);
    }
    if (actual != key) {
      data->clear();
      return false;
    }
  }
  char extra = 0;
  if (input.read(&extra, 1)) {
    data->clear();
    return false;
  }
  return true;
}

uint64_t committed_bytes(const fs::path &directory) {
  uint64_t total = 0;
  std::error_code error;
  for (fs::recursive_directory_iterator it(directory, error), end; it != end;
       it.increment(error)) {
    if (error) {
      error.clear();
      continue;
    }
    if (!it->is_regular_file(error) || it->path().extension() != ".chunk") {
      continue;
    }
    uintmax_t size = it->file_size(error);
    if (!error && size >= sizeof(cache_header) &&
        size - sizeof(cache_header) <= UINT64_MAX - total) {
      total += static_cast<uint64_t>(size - sizeof(cache_header));
    }
    error.clear();
  }
  return total;
}

void evict_until_fits(const fs::path &directory, uint64_t limit,
                      uint64_t incoming, const fs::path &protected_path) {
  if (incoming > limit) {
    return;
  }
  while (committed_bytes(directory) > limit - incoming) {
    fs::path victim;
    fs::file_time_type oldest;
    bool found = false;
    std::error_code error;
    for (fs::recursive_directory_iterator it(directory, error), end; it != end;
         it.increment(error)) {
      if (error) {
        error.clear();
        continue;
      }
      if (!it->is_regular_file(error) || it->path().extension() != ".chunk" ||
          it->path() == protected_path) {
        continue;
      }
      auto modified = it->last_write_time(error);
      if (!error && (!found || modified < oldest)) {
        oldest = modified;
        victim = it->path();
        found = true;
      }
      error.clear();
    }
    if (!found) {
      return;
    }
    fs::remove(victim, error);
  }
}

void cache_worker() {
  cache_state &value = state();
  for (;;) {
    cache_job job;
    {
      std::unique_lock<std::mutex> lock(value.queue_mutex);
      value.queue_condition.wait(lock,
                                 [&] { return !value.queue.empty(); });
      job = std::move(value.queue.front());
      value.queue.pop_front();
      value.queued_bytes -= job.data.size();
      value.worker_busy = true;
      value.queue_condition.notify_all();
    }

    lupine_dedup_cache_admit_encoded(job.key, job.data.data(), job.data.size(),
                                     job.lz4_compressed);

    {
      std::lock_guard<std::mutex> lock(value.queue_mutex);
      value.worker_busy = false;
    }
    value.queue_condition.notify_all();
  }
}

} // namespace

bool lupine_dedup_cache_enabled() { return enabled(); }

bool lupine_dedup_cache_lookup(const lupine_dedup_key &key,
                               std::vector<unsigned char> *data) {
  if (data == nullptr || !enabled()) {
    return false;
  }
  cache_lock lock(state().directory);
  if (!lock.acquired()) {
    return false;
  }
  fs::path path = entry_path(key);
  if (read_entry(path, key, data)) {
    std::error_code error;
    auto mtime_start = std::chrono::steady_clock::now();
    fs::last_write_time(path, fs::file_time_type::clock::now(), error);
    record_mtime(mtime_start);
    return true;
  }
  data->clear();
  std::error_code error;
  if (fs::exists(path, error)) {
    fs::remove(path, error);
  }
  return false;
}

void lupine_dedup_cache_admit_encoded(const lupine_dedup_key &key,
                                      const unsigned char *data, size_t bytes,
                                      bool lz4_compressed) {
  if (!enabled() || data == nullptr || key.bytes == 0 || bytes == 0 ||
      bytes > state().limit || bytes >
          static_cast<size_t>(std::numeric_limits<std::streamsize>::max())) {
    return;
  }
  cache_lock lock(state().directory);
  if (!lock.acquired()) {
    return;
  }
  fs::path path = entry_path(key);
  std::error_code error;
  fs::create_directories(path.parent_path(), error);
  if (error) {
    return;
  }
  std::vector<unsigned char> existing;
  if (read_entry(path, key, &existing)) {
    auto mtime_start = std::chrono::steady_clock::now();
    fs::last_write_time(path, fs::file_time_type::clock::now(), error);
    record_mtime(mtime_start);
    return;
  }
  if (fs::exists(path, error)) {
    fs::remove(path, error);
  }

  evict_until_fits(state().directory, state().limit, bytes, path);
  if (committed_bytes(state().directory) > state().limit - bytes) {
    return;
  }

  static std::atomic<uint64_t> sequence{0};
  fs::path temporary = path;
  temporary += ".tmp." + std::to_string(static_cast<unsigned long long>(
                                      sequence.fetch_add(1))) +
                "." + std::to_string(static_cast<unsigned long long>(
                                      std::hash<std::thread::id>{}(
                                          std::this_thread::get_id())));
  cache_header header = {};
  std::memcpy(header.magic, kMagic, sizeof(header.magic));
  header.version = kVersion;
  header.hash_low = key.hash_low;
  header.hash_high = key.hash_high;
  header.bytes = key.bytes;
  header.payload_bytes = bytes;
  header.flags = lz4_compressed ? kFlagLz4 : 0;
  {
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    if (!output || !output.write(reinterpret_cast<const char *>(&header),
                                 sizeof(header)) ||
        !output.write(reinterpret_cast<const char *>(data),
                      static_cast<std::streamsize>(bytes))) {
      output.close();
      fs::remove(temporary, error);
      return;
    }
  }
  fs::rename(temporary, path, error);
  if (error) {
    fs::remove(temporary, error);
    return;
  }
  evict_until_fits(state().directory, state().limit, bytes, path);
}

bool lupine_dedup_cache_enqueue_encoded(const lupine_dedup_key &key,
                                        std::vector<unsigned char> data,
                                        bool lz4_compressed) {
  if (!enabled() || key.bytes == 0 || data.empty() ||
      data.size() > state().limit ||
      data.size() > static_cast<size_t>(
                          std::numeric_limits<std::streamsize>::max())) {
    return false;
  }

  cache_state &value = state();
  {
    std::lock_guard<std::mutex> lock(value.queue_mutex);
    if (data.size() > value.queue_limit ||
        value.queued_bytes > value.queue_limit - data.size()) {
      return false;
    }
    value.queued_bytes += data.size();
    value.queue.push_back(
        cache_job{key, std::move(data), lz4_compressed});
  }
  value.queue_condition.notify_one();
  return true;
}

void lupine_dedup_cache_flush() {
  if (!enabled()) {
    return;
  }
  cache_state &value = state();
  std::unique_lock<std::mutex> lock(value.queue_mutex);
  value.queue_condition.wait(lock,
                             [&] {
                               return value.queue.empty() &&
                                      !value.worker_busy;
                             });
}
