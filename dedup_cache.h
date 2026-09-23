#ifndef LUPINE_DEDUP_CACHE_H
#define LUPINE_DEDUP_CACHE_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "dedup_protocol.h"

bool lupine_dedup_cache_enabled();
bool lupine_dedup_cache_lookup(const lupine_dedup_key &key,
                               std::vector<unsigned char> *data);
void lupine_dedup_cache_admit_encoded(const lupine_dedup_key &key,
                                      const unsigned char *data,
                                      size_t bytes, bool lz4_compressed);
bool lupine_dedup_cache_enqueue_encoded(const lupine_dedup_key &key,
                                        std::vector<unsigned char> data,
                                        bool lz4_compressed);
void lupine_dedup_cache_flush();

#endif
