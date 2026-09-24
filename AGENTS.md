# Repository memory

## Containerized Lupine integration testing

- Run the LUPINE server in a GPU-enabled container with GPU access passed through.
- Run the LUPINE client in a GPU-disabled container; it must access GPU functionality only through the LUPINE network connection to the server.

## Local integration image tags

- Client image: `angzam78/lupine-dedup-client`
- ComfyUI client image: `angzam78/lupine-dedup-comfyui`
- Server image: `angzam78/lupine-dedup-server`

These are local deduplication-extension integration-test tags. Keep them separate from the generic published Lupine image names used by the tracked project documentation.


## Deduplication implementation

- Deduplication is negotiated through the existing HTTP/2 handshake and is enabled by default with `/var/cache/lupine/dedup` and a `32GiB` limit; override with `LUPINE_DEDUP_CACHE_DIR` and `LUPINE_DEDUP_CACHE_SIZE`, or set the size to `0` to disable it.
- Async HtoD deduplication is present but remains conservative: it requires a full pageable client push, an individual transfer of at least 8 MiB, no capture/callback conflict, negotiated peer support, and available bulk lanes.
- Valid ComfyUI GPU-over-IP testing requires the client container to omit `--gpus all` and expose GPUs only through `LUPINE_SERVER`; otherwise workloads execute locally and are invalid for bandwidth testing.
- A valid ComfyUI run produced 1,286 remote `cuMemcpyHtoDAsync_v2` calls; 554 transfers were at least 8 MiB but all were server-authoritative (`pushed=0`), while only 5.84 MiB total was pageable-pushed. No dedup RPC or cache entry was produced.
- Validate with a CUDA 12.8 development container: build `lupine_driver_server` and `lupine_cuda_client`, then run `h2_test`, `dispatch_test`, `transport_test`, and `dedup_cache_test`.
- Dedup cache writes use a bounded asynchronous worker queue (default 1 GiB, configurable with `LUPINE_DEDUP_CACHE_QUEUE_SIZE`); transfer readiness does not wait for disk admission, and cache lookup sees entries only after atomic rename. `lupine_dedup_cache_flush()` makes queued writes deterministic in tests.
- Cache payload integrity verification defaults to disabled for production performance and can be enabled with `LUPINE_DEDUP_CACHE_VERIFY=1`; `LUPINE_DEDUP_PROFILE=1` reports cache read, file, decompression, hash, and LRU timestamp timings.

## Terminal handling

- Do not use explicit shell `exit` commands in terminal tool calls; preserve the terminal session and report status through command output instead.
