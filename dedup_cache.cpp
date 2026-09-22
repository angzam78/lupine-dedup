#include "dedup_cache.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>

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
};

constexpr char kMagic[] = "LUPDEDUP";
constexpr uint32_t kVersion = 1;

struct cache_state {
  std::once_flag once;
  bool enabled = false;
  uint64_t limit = 0;
  fs::path directory;
};

cache_state &state() {
  static auto *value = new cache_state;
  return *value;
}

void initialize() {
  cache_state &value = state();
  const char *directory = std::getenv("LUPINE_DEDUP_CACHE_DIR");
  const char *size = std::getenv("LUPINE_DEDUP_CACHE_SIZE");
  uint64_t limit = 0;
  if (directory == nullptr || size == nullptr ||
      !lupine_parse_size(size, &limit) || limit == 0) {
    return;
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
  value.enabled = true;
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
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return false;
  }
  cache_header header = {};
  input.read(reinterpret_cast<char *>(&header), sizeof(header));
  if (!input || std::memcmp(header.magic, kMagic, sizeof(header.magic)) != 0 ||
      header.version != kVersion || header.hash_low != key.hash_low ||
      header.hash_high != key.hash_high || header.bytes != key.bytes) {
    return false;
  }
  std::error_code error;
  uintmax_t file_size = fs::file_size(path, error);
  if (error || key.bytes > static_cast<uint64_t>(SIZE_MAX) ||
      key.bytes > static_cast<uint64_t>(std::numeric_limits<std::streamsize>::max()) ||
      file_size != sizeof(cache_header) + key.bytes) {
    return false;
  }
  data->resize(static_cast<size_t>(key.bytes));
  if (key.bytes != 0) {
    input.read(reinterpret_cast<char *>(data->data()),
               static_cast<std::streamsize>(key.bytes));
  }
  if (!input || lupine_dedup_hash(data->data(), data->size()) != key) {
    data->clear();
    return false;
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
    fs::last_write_time(path, fs::file_time_type::clock::now(), error);
    return true;
  }
  data->clear();
  std::error_code error;
  if (fs::exists(path, error)) {
    fs::remove(path, error);
  }
  return false;
}

void lupine_dedup_cache_admit(const lupine_dedup_key &key,
                              const unsigned char *data) {
  if (!enabled() || data == nullptr || key.bytes == 0 ||
      key.bytes > state().limit || key.bytes > SIZE_MAX) {
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
    fs::last_write_time(path, fs::file_time_type::clock::now(), error);
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
  {
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    if (!output || !output.write(reinterpret_cast<const char *>(&header),
                                 sizeof(header)) ||
        !output.write(reinterpret_cast<const char *>(data),
                      static_cast<std::streamsize>(key.bytes))) {
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
  evict_until_fits(state().directory, state().limit, key.bytes, path);
}
