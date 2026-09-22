#include "dedup_cache.h"

#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
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
  set_environment("LUPINE_DEDUP_CACHE_SIZE", "64B");
  assert(lupine_dedup_cache_enabled());

  std::vector<unsigned char> first(48, 0x11);
  std::vector<unsigned char> second(48, 0x22);
  auto first_key = lupine_dedup_hash(first.data(), first.size());
  auto second_key = lupine_dedup_hash(second.data(), second.size());
  lupine_dedup_cache_admit(first_key, first.data());
  std::vector<unsigned char> loaded;
  assert(lupine_dedup_cache_lookup(first_key, &loaded));
  assert(loaded == first);

  lupine_dedup_cache_admit(second_key, second.data());
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
