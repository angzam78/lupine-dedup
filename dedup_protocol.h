#ifndef LUPINE_DEDUP_PROTOCOL_H
#define LUPINE_DEDUP_PROTOCOL_H

#include <cstddef>
#include <cstdint>

#include "third_party/lz4/lib/xxhash.h"

static constexpr uint16_t LUPINE_DEDUP_PROTOCOL_VERSION = 1;
static constexpr uint32_t LUPINE_DEDUP_CHUNK_BYTES =
    4u * 1024u * 1024u;
static constexpr uint32_t LUPINE_DEDUP_MAX_CHUNKS = 4u * 1024u * 1024u;
static constexpr int LUPINE_RPC_DEDUP_TRANSFER = 0x4c445450;
static constexpr int LUPINE_RPC_DEDUP_BULK_CHUNK = 0x4c444243;
static constexpr int LUPINE_RPC_DEDUP_COMMIT = 0x4c44434d;
static constexpr int LUPINE_RPC_DEDUP_COMMIT_ASYNC = 0x4c444341;

enum : uint8_t {
  LUPINE_DEDUP_COMPRESSION_NONE = 0,
  LUPINE_DEDUP_COMPRESSION_LZ4 = 1,
};

enum : uint8_t {
  LUPINE_DEDUP_BEGIN = 1,
  LUPINE_DEDUP_HASH = 2,
  LUPINE_DEDUP_HASH_END = 3,
  LUPINE_DEDUP_ABORT = 4,
  LUPINE_DEDUP_HIT = 5,
  LUPINE_DEDUP_MISS = 6,
  LUPINE_DEDUP_ERROR = 7,
  LUPINE_DEDUP_READY = 8,
  LUPINE_DEDUP_DONE = 9,
};

struct lupine_dedup_record_header {
  uint8_t type;
  uint8_t flags;
  uint16_t version;
  uint32_t length;
};

struct lupine_dedup_begin {
  uint64_t copy_id;
  uint16_t version;
  uint16_t reserved16;
  uint64_t total_bytes;
  uint64_t destination;
  uint32_t chunk_bytes;
  uint32_t chunk_count;
  uint8_t direction;
  uint8_t reserved[7];
};

struct lupine_dedup_hash_record {
  uint64_t sequence;
  uint64_t offset;
  uint64_t bytes;
  uint64_t hash_low;
  uint64_t hash_high;
};

struct lupine_dedup_result_record {
  uint64_t sequence;
};

struct lupine_dedup_done_record {
  int32_t result;
  uint32_t reserved;
};

struct lupine_dedup_key {
  uint64_t hash_low;
  uint64_t hash_high;
  uint64_t bytes;

  bool operator==(const lupine_dedup_key &other) const {
    return hash_low == other.hash_low && hash_high == other.hash_high &&
           bytes == other.bytes;
  }

  bool operator!=(const lupine_dedup_key &other) const {
    return !(*this == other);
  }
};

struct lupine_dedup_bulk_chunk_header {
  int request_id;
  int op;
  uint64_t copy_id;
  uint64_t sequence;
  uint64_t total_bytes;
  uint64_t offset;
  uint64_t bytes;
  uint64_t hash_low;
  uint64_t hash_high;
  uint8_t compression;
  uint8_t reserved[7];
};

inline lupine_dedup_key lupine_dedup_hash(const void *data, size_t bytes) {
  return {static_cast<uint64_t>(XXH64(data, bytes, 0x9e3779b185ebca87ULL)),
          static_cast<uint64_t>(XXH64(data, bytes, 0xc2b2ae3d27d4eb4fULL)),
          static_cast<uint64_t>(bytes)};
}

inline size_t lupine_dedup_chunk_count(size_t bytes) {
  return bytes == 0 ? 0 : (bytes - 1) / LUPINE_DEDUP_CHUNK_BYTES + 1;
}

#endif
