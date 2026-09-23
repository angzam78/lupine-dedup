#include "cuda_server.h"
#include "cuda_server_memcpy.h"

#include <algorithm>
#include <climits>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dedup_cache.h"
#include "dedup_protocol.h"
#include "lupine_log.h"
#include "third_party/lz4/lib/lz4.h"

namespace {

struct dedup_staging {
  uint64_t copy_id = 0;
  uint64_t destination = 0;
  size_t total_bytes = 0;
  uint8_t direction = LUPINE_COPY_DIRECTION_HTOD;
  std::unique_ptr<unsigned char[]> data;
  std::vector<lupine_dedup_key> keys;
  std::vector<unsigned char> ready;
  bool failed = false;
};

std::mutex &staging_mutex() {
  static auto *mutex = new std::mutex;
  return *mutex;
}

std::condition_variable &staging_progress() {
  static auto *condition = new std::condition_variable;
  return *condition;
}

std::unordered_map<uint64_t, std::shared_ptr<dedup_staging>> &stagings() {
  static auto *map = new std::unordered_map<
      uint64_t, std::shared_ptr<dedup_staging>>;
  return *map;
}

bool range_for_sequence(const dedup_staging &staging, uint64_t sequence,
                        uint64_t offset, uint64_t bytes) {
  if (sequence >= staging.keys.size() || bytes == 0 || bytes > SIZE_MAX ||
      offset > staging.total_bytes || bytes > staging.total_bytes - offset) {
    return false;
  }
  uint64_t expected_offset = sequence * LUPINE_DEDUP_CHUNK_BYTES;
  if (expected_offset / LUPINE_DEDUP_CHUNK_BYTES != sequence ||
      expected_offset != offset) {
    return false;
  }
  size_t expected_bytes = std::min<size_t>(
      LUPINE_DEDUP_CHUNK_BYTES, staging.total_bytes - expected_offset);
  return bytes == expected_bytes && staging.keys[sequence].bytes == bytes;
}

std::shared_ptr<dedup_staging> find_staging(uint64_t copy_id) {
  std::lock_guard<std::mutex> lock(staging_mutex());
  auto it = stagings().find(copy_id);
  return it == stagings().end() ? nullptr : it->second;
}

void fail_staging(uint64_t copy_id) {
  std::lock_guard<std::mutex> lock(staging_mutex());
  auto it = stagings().find(copy_id);
  if (it != stagings().end()) {
    it->second->failed = true;
  }
  staging_progress().notify_all();
}

bool staging_ready(const dedup_staging &staging) {
  for (unsigned char ready : staging.ready) {
    if (ready != 1) {
      return false;
    }
  }
  return true;
}

int write_manifest_response(conn_t *conn, int request_id,
                            const std::vector<unsigned char> &results) {
  uint32_t count = static_cast<uint32_t>(results.size());
  if (rpc_write_start_response(conn, request_id) < 0 ||
      rpc_write(conn, &count, sizeof(count)) < 0) {
    return -1;
  }
  if (!results.empty() && rpc_write(conn, results.data(), results.size()) < 0) {
    return -1;
  }
  return rpc_write_end(conn) < 0 ? -1 : 0;
}

} // namespace

int handle_lupineDedupTransfer(conn_t *conn) {
  lupine_dedup_record_header begin_header = {};
  lupine_dedup_begin begin = {};
  if (rpc_read(conn, &begin_header, sizeof(begin_header)) < 0 ||
      begin_header.type != LUPINE_DEDUP_BEGIN ||
      begin_header.version != LUPINE_DEDUP_PROTOCOL_VERSION ||
      begin_header.length != sizeof(begin) ||
      rpc_read(conn, &begin, sizeof(begin)) < 0 ||
      begin.version != LUPINE_DEDUP_PROTOCOL_VERSION ||
      (begin.direction != LUPINE_COPY_DIRECTION_HTOD &&
       begin.direction != LUPINE_COPY_DIRECTION_HTOH) ||
      begin.chunk_bytes != LUPINE_DEDUP_CHUNK_BYTES ||
      begin.chunk_count == 0 || begin.chunk_count > LUPINE_DEDUP_MAX_CHUNKS ||
      begin.total_bytes == 0 || begin.total_bytes > SIZE_MAX ||
      begin.chunk_count != lupine_dedup_chunk_count(begin.total_bytes)) {
    return -1;
  }

  auto staging = std::make_shared<dedup_staging>();
  staging->copy_id = begin.copy_id;
  staging->destination = begin.destination;
  staging->total_bytes = static_cast<size_t>(begin.total_bytes);
  staging->direction = begin.direction;
  staging->data.reset(new (std::nothrow) unsigned char[staging->total_bytes]);
  if (staging->data == nullptr) {
    return -1;
  }
  try {
    staging->keys.resize(begin.chunk_count);
    staging->ready.resize(begin.chunk_count);
  } catch (...) {
    return -1;
  }

  std::vector<unsigned char> results;
  try {
    results.reserve(static_cast<size_t>(begin.chunk_count) *
                    (sizeof(lupine_dedup_record_header) +
                     sizeof(lupine_dedup_result_record)));
  } catch (...) {
    return -1;
  }

  for (uint32_t index = 0; index < begin.chunk_count; ++index) {
    lupine_dedup_record_header header = {};
    lupine_dedup_hash_record hash = {};
    if (rpc_read(conn, &header, sizeof(header)) < 0 ||
        header.type != LUPINE_DEDUP_HASH ||
        header.version != LUPINE_DEDUP_PROTOCOL_VERSION ||
        header.length != sizeof(hash) ||
        rpc_read(conn, &hash, sizeof(hash)) < 0) {
      return -1;
    }
    lupine_dedup_key key = {hash.hash_low, hash.hash_high, hash.bytes};
    staging->keys[index] = key;
    if (hash.sequence != index ||
        !range_for_sequence(*staging, hash.sequence, hash.offset, hash.bytes)) {
      return -1;
    }

    std::vector<unsigned char> cached;
    bool hit = lupine_dedup_cache_lookup(key, &cached);
    if (hit) {
      std::memcpy(staging->data.get() + hash.offset, cached.data(),
                  cached.size());
      staging->ready[index] = 1;
    }
    lupine_dedup_record_header result_header = {
        static_cast<uint8_t>(hit ? LUPINE_DEDUP_HIT : LUPINE_DEDUP_MISS), 0,
        LUPINE_DEDUP_PROTOCOL_VERSION,
        static_cast<uint32_t>(sizeof(lupine_dedup_result_record))};
    lupine_dedup_result_record result = {hash.sequence};
    const auto *header_bytes = reinterpret_cast<const unsigned char *>(
        &result_header);
    const auto *result_bytes =
        reinterpret_cast<const unsigned char *>(&result);
    results.insert(results.end(), header_bytes,
                   header_bytes + sizeof(result_header));
    results.insert(results.end(), result_bytes,
                   result_bytes + sizeof(result));
  }

  lupine_dedup_record_header end = {};
  if (rpc_read(conn, &end, sizeof(end)) < 0 ||
      end.type != LUPINE_DEDUP_HASH_END ||
      end.version != LUPINE_DEDUP_PROTOCOL_VERSION || end.length != 0) {
    return -1;
  }
  int request_id = rpc_read_end(conn);
  if (request_id < 0) {
    return -1;
  }

  {
    std::lock_guard<std::mutex> lock(staging_mutex());
    if (!stagings().emplace(begin.copy_id, staging).second) {
      return -1;
    }
  }
  if (write_manifest_response(conn, request_id, results) < 0) {
    fail_staging(begin.copy_id);
    return -1;
  }
  return 0;
}

int handle_lupineDedupBulkChunk(conn_t *conn) {
  lupine_dedup_bulk_chunk_header header = {};
  if (rpc_read(conn, &header.copy_id, sizeof(header.copy_id)) < 0 ||
      rpc_read(conn, &header.sequence, sizeof(header.sequence)) < 0 ||
      rpc_read(conn, &header.total_bytes, sizeof(header.total_bytes)) < 0 ||
      rpc_read(conn, &header.offset, sizeof(header.offset)) < 0 ||
      rpc_read(conn, &header.bytes, sizeof(header.bytes)) < 0 ||
      rpc_read(conn, &header.hash_low, sizeof(header.hash_low)) < 0 ||
      rpc_read(conn, &header.hash_high, sizeof(header.hash_high)) < 0 ||
      rpc_read(conn, &header.compression, sizeof(header.compression)) < 0 ||
      rpc_read(conn, &header.reserved, sizeof(header.reserved)) < 0) {
    return -1;
  }
  auto staging = find_staging(header.copy_id);
  bool valid = staging != nullptr;
  size_t raw_bytes = 0;
  if (valid) {
    std::lock_guard<std::mutex> lock(staging_mutex());
    valid = !staging->failed && header.total_bytes == staging->total_bytes &&
            header.sequence < staging->keys.size() &&
            staging->ready[header.sequence] == 0;
    if (valid) {
      raw_bytes = static_cast<size_t>(staging->keys[header.sequence].bytes);
      valid = range_for_sequence(*staging, header.sequence, header.offset,
                                 raw_bytes) &&
              staging->keys[header.sequence].hash_low == header.hash_low &&
              staging->keys[header.sequence].hash_high == header.hash_high &&
              (header.compression == LUPINE_DEDUP_COMPRESSION_NONE ||
               header.compression == LUPINE_DEDUP_COMPRESSION_LZ4);
    }
    if (valid) {
      staging->ready[header.sequence] = 2;
    }
  }
  if (!valid || header.bytes > SIZE_MAX || header.bytes == 0 ||
      header.bytes > LZ4_compressBound(LUPINE_DEDUP_CHUNK_BYTES)) {
    if (header.bytes > SIZE_MAX ||
        rpc_drain(conn, header.bytes > SIZE_MAX ? 0
                                                 : static_cast<size_t>(header.bytes)) <
            0 ||
        rpc_read_end(conn) < 0) {
      return -1;
    }
    fail_staging(header.copy_id);
    return 0;
  }

  std::vector<unsigned char> encoded(static_cast<size_t>(header.bytes));
  if (rpc_read(conn, encoded.data(), encoded.size()) < 0 ||
      rpc_read_end(conn) < 0) {
    fail_staging(header.copy_id);
    return -1;
  }
  std::vector<unsigned char> decoded(raw_bytes);
  if (header.compression == LUPINE_DEDUP_COMPRESSION_LZ4) {
    if (raw_bytes > static_cast<size_t>(INT_MAX) ||
        encoded.size() > static_cast<size_t>(INT_MAX) ||
        LZ4_decompress_safe(reinterpret_cast<const char *>(encoded.data()),
                            reinterpret_cast<char *>(decoded.data()),
                            static_cast<int>(encoded.size()),
                            static_cast<int>(raw_bytes)) !=
            static_cast<int>(raw_bytes)) {
      fail_staging(header.copy_id);
      return 0;
    }
  } else if (encoded.size() != raw_bytes) {
    fail_staging(header.copy_id);
    return 0;
  } else {
    std::memcpy(decoded.data(), encoded.data(), raw_bytes);
  }
  lupine_dedup_key actual = lupine_dedup_hash(decoded.data(), decoded.size());
  if (actual.hash_low != header.hash_low || actual.hash_high != header.hash_high ||
      actual.bytes != raw_bytes) {
    fail_staging(header.copy_id);
    return 0;
  }
  {
    std::lock_guard<std::mutex> lock(staging_mutex());
    if (staging->failed || staging->ready[header.sequence] != 2) {
      staging->failed = true;
      staging_progress().notify_all();
      return 0;
    }
    std::memcpy(staging->data.get() + header.offset, decoded.data(), raw_bytes);
    staging->ready[header.sequence] = 1;
  }
  if (!lupine_dedup_cache_enqueue_encoded(
          actual, std::move(encoded),
          header.compression == LUPINE_DEDUP_COMPRESSION_LZ4)) {
    LUPINE_TRACE_LOG("LUPINE dedup cache queue full; continuing without admission");
  }
  staging_progress().notify_all();
  return 0;
}

int handle_lupineDedupCommit(conn_t *conn) {
  uint64_t copy_id = 0;
  if (rpc_read(conn, &copy_id, sizeof(copy_id)) < 0) {
    return -1;
  }
  int request_id = rpc_read_end(conn);
  if (request_id < 0) {
    return -1;
  }
  auto staging = find_staging(copy_id);
  CUresult result = CUDA_ERROR_INVALID_VALUE;
  if (staging != nullptr) {
    std::unique_lock<std::mutex> lock(staging_mutex());
    staging_progress().wait(lock, [&] {
      return staging->failed || staging_ready(*staging);
    });
    if (!staging->failed) {
      if (staging->direction == LUPINE_COPY_DIRECTION_HTOH) {
        std::memcpy(reinterpret_cast<void *>(staging->destination),
                    staging->data.get(), staging->total_bytes);
        result = CUDA_SUCCESS;
      } else {
        result = cuMemcpyHtoD_v2(
            static_cast<CUdeviceptr>(staging->destination), staging->data.get(),
            staging->total_bytes);
      }
    } else {
      result = CUDA_ERROR_DEVICE_UNAVAILABLE;
    }
    stagings().erase(copy_id);
  }
  if (rpc_write_start_response(conn, request_id) < 0 ||
      rpc_write(conn, &result, sizeof(result)) < 0 || rpc_write_end(conn) < 0) {
    return -1;
  }
  return 0;
}

int handle_lupineDedupCommitAsync(conn_t *conn) {
  uint64_t copy_id = 0;
  CUstream stream = nullptr;
  if (rpc_read(conn, &copy_id, sizeof(copy_id)) < 0 ||
      rpc_read(conn, &stream, sizeof(stream)) < 0) {
    return -1;
  }
  int request_id = rpc_read_end(conn);
  if (request_id < 0) {
    return -1;
  }

  auto staging = find_staging(copy_id);
  CUresult result = CUDA_ERROR_INVALID_VALUE;
  if (staging != nullptr) {
    std::unique_lock<std::mutex> lock(staging_mutex());
    staging_progress().wait(lock, [&] {
      return staging->failed || staging_ready(*staging);
    });
    if (!staging->failed &&
        staging->direction == LUPINE_COPY_DIRECTION_HTOD) {
      void *data = staging->data.release();
      result = lupine_server_enqueue_dedup_htod_async(
          stream, static_cast<CUdeviceptr>(staging->destination), data,
          staging->total_bytes);
    } else if (staging->failed) {
      result = CUDA_ERROR_DEVICE_UNAVAILABLE;
    } else {
      result = CUDA_ERROR_INVALID_VALUE;
    }
    stagings().erase(copy_id);
  }
  if (rpc_write_start_response(conn, request_id) < 0 ||
      rpc_write(conn, &result, sizeof(result)) < 0 || rpc_write_end(conn) < 0) {
    return -1;
  }
  return 0;
}


void lupine_server_dedup_bulk_connection_lost() {
  std::lock_guard<std::mutex> lock(staging_mutex());
  for (auto &entry : stagings()) {
    entry.second->failed = true;
  }
  staging_progress().notify_all();
}
