#include "dedup_cache.h"

#include <cassert>
#include "third_party/lz4/lib/lz4.h"
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

namespace {

void set_environment(const char *name, const std::string &value) {
#ifdef _WIN32
  _putenv_s(name, value.c_str());
#else
  setenv(name, value.c_str(), 1);
#endif
}

} // namespace

int main() {
  std::filesystem::path directory =
      std::filesystem::temp_directory_path() /
      ("lupine-dedup-cache-test-" + std::to_string(
                                      static_cast<unsigned long long>(
#ifdef _WIN32
                                          1
#else
                                          getpid()
#endif
                                          )));
  std::filesystem::remove_all(directory);
  set_environment("LUPINE_DEDUP_CACHE_DIR", directory.string());
  set_environment("LUPINE_DEDUP_CACHE_SIZE", "48B");
  assert(lupine_dedup_cache_enabled());

  std::vector<unsigned char> first(48, 0x11);
  std::vector<unsigned char> second(48, 0x22);
  auto first_key = lupine_dedup_hash(first.data(), first.size());
  auto second_key = lupine_dedup_hash(second.data(), second.size());
  std::vector<unsigned char> first_encoded(
      static_cast<size_t>(LZ4_compressBound(static_cast<int>(first.size()))));
  int first_compressed = LZ4_compress_default(
      reinterpret_cast<const char *>(first.data()),
      reinterpret_cast<char *>(first_encoded.data()), static_cast<int>(first.size()),
      static_cast<int>(first_encoded.size()));
  assert(first_compressed > 0);
  first_encoded.resize(static_cast<size_t>(first_compressed));
  lupine_dedup_cache_admit_encoded(first_key, first_encoded.data(),
                                   first_encoded.size(), true);
  std::vector<unsigned char> loaded;
  assert(lupine_dedup_cache_lookup(first_key, &loaded));
  assert(loaded == first);

  std::vector<unsigned char> queued(48, 0x33);
  auto queued_key = lupine_dedup_hash(queued.data(), queued.size());
  std::vector<unsigned char> queued_encoded(
      static_cast<size_t>(LZ4_compressBound(static_cast<int>(queued.size()))));
  int queued_compressed = LZ4_compress_default(
      reinterpret_cast<const char *>(queued.data()),
      reinterpret_cast<char *>(queued_encoded.data()),
      static_cast<int>(queued.size()), static_cast<int>(queued_encoded.size()));
  assert(queued_compressed > 0);
  queued_encoded.resize(static_cast<size_t>(queued_compressed));
  assert(lupine_dedup_cache_enqueue_encoded(queued_key,
                                             std::move(queued_encoded), true));
  lupine_dedup_cache_flush();
  loaded.clear();
  assert(lupine_dedup_cache_lookup(queued_key, &loaded));
  assert(loaded == queued);

  lupine_dedup_cache_admit_encoded(second_key, second.data(), second.size(),
                                   false);
  loaded.clear();
  assert(!lupine_dedup_cache_lookup(first_key, &loaded));
  assert(lupine_dedup_cache_lookup(second_key, &loaded));
  assert(loaded == second);

  for (const auto &entry : std::filesystem::recursive_directory_iterator(
           directory)) {
    if (entry.path().extension() == ".chunk") {
      std::ofstream corrupt(entry.path(), std::ios::binary | std::ios::trunc);
      corrupt << "bad";
      break;
    }
  }
  assert(!lupine_dedup_cache_lookup(second_key, &loaded));
  std::filesystem::remove_all(directory);
  return 0;
}
