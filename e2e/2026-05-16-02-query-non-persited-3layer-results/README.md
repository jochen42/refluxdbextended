
## Hardware

```txt
Model Name:	MacBook Pro
Model Identifier:	Mac16,7
Model Number:	MX2Y3D/A
Chip:	Apple M4 Pro
Total Number of Cores:	14 (10 Performance and 4 Efficiency)
Memory:	48 GB
```
## git commit

dirty@2

## Command

```bash
PARALLEL=4 WRITER_PORT=18181 QUERIER_PORT=18281 MINIO_PORT=19000 MINIO_CONSOLE_PORT=19001  HOURS=2400 HOSTS=2000 POINTS_PER_HOST=500 BATCH_SIZE=5000 ./run-bench.sh
[16:41:47] phase 0: starting writer + querier (no compactor)
[+] up 6/6
 ✔ Network e2e_default         Created                                                                       0.0s
 ✔ Volume e2e_minio-data       Created                                                                       0.0s
 ✔ Container e2e-minio-1       Healthy                                                                       6.5s
 ✔ Container e2e-minio-setup-1 Exited                                                                        6.4s
 ✔ Container e2e-writer-1      Healthy                                                                       6.4s
 ✔ Container e2e-querier-1     Healthy                                                                       8.4s
[16:41:55] phase 1: generating data (hours=2400 hosts=2000 points-per-host=500 batch-size=5000)
[+]  3/3t 3/32
 ✔ Container e2e-minio-1       Healthy                                                                       0.5s
 ✔ Container e2e-writer-1      Running                                                                       0.0s
 ✔ Container e2e-minio-setup-1 Exited                                                                        0.6s
Container e2e-writer-1 Waiting
Container e2e-writer-1 Healthy
Container e2e-gen-run-acc0168bed8f Creating
Container e2e-gen-run-acc0168bed8f Created
plan: 2000 hosts × 500 points = 1000000 total points over 2400h (start=1770309717986080964 end=1778949717986080964) parallel=4
  progress: 100000/1000000 (10.0%) at 20337 points/s
  progress: 200000/1000000 (20.0%) at 20224 points/s
  progress: 300000/1000000 (30.0%) at 20123 points/s
  progress: 400000/1000000 (40.0%) at 20103 points/s
  progress: 500000/1000000 (50.0%) at 20072 points/s
  progress: 600000/1000000 (60.0%) at 20061 points/s
  progress: 700000/1000000 (70.0%) at 20047 points/s
  progress: 800000/1000000 (80.0%) at 20046 points/s
  progress: 900000/1000000 (90.0%) at 20040 points/s
  progress: 1000000/1000000 (100.0%) at 20046 points/s
done: wrote 1000000 points in 49.9s (20046 points/s)
[16:42:48] phase 1b: waiting for writer to flush WAL + publish inventory
[16:44:48] phase 2: benchmarking uncompacted dataset
[+]  4/4t 4/43
 ✔ Container e2e-minio-1       Healthy                                                                       0.5s
 ✔ Container e2e-writer-1      Healthy                                                                       1.6s
 ✔ Container e2e-querier-1     Running                                                                       0.0s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.1s
Container e2e-querier-1 Waiting
Container e2e-querier-1 Healthy
Container e2e-bench-run-fabafec09bc9 Creating
Container e2e-bench-run-fabafec09bc9 Created
benchmarking 4 queries × 5 runs (tag=uncompacted)
  full_scan: warming...
    run 1/5: 120.551s (186 bytes)
    run 2/5: 120.780s (185 bytes)
    run 3/5: 118.407s (185 bytes)
    run 4/5: 118.457s (184 bytes)
    run 5/5: 118.068s (185 bytes)
  high_card_filter: warming...
    run 1/5: 12.518s (1436 bytes)
    run 2/5: 12.514s (1436 bytes)
    run 3/5: 12.600s (1436 bytes)
    run 4/5: 12.658s (1436 bytes)
    run 5/5: 13.127s (1436 bytes)
  hour_buckets: warming...
    run 1/5: 40.242s (55763 bytes)
    run 2/5: 40.218s (55763 bytes)
    run 3/5: 40.299s (55739 bytes)
    run 4/5: 40.343s (55763 bytes)
    run 5/5: 40.737s (56031 bytes)
  narrow_window: warming...
    run 1/5: 0.102s (168 bytes)
    run 2/5: 0.119s (166 bytes)
    run 3/5: 0.102s (169 bytes)
    run 4/5: 0.096s (167 bytes)
    run 5/5: 0.101s (167 bytes)
wrote results/uncompacted.json
[17:02:06] uncompacted parquet object count (best effort):        0
unknown
[17:02:06] phase 3: starting compactor; will poll for settle
[+] up 3/3
 ✔ Container e2e-minio-1       Healthy                                                                       1.7s
 ✔ Container e2e-compactor-1   Healthy                                                                       1.7s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.1s
[17:02:08] compactor cycles observed so far: 0
[17:02:18] compactor cycles observed so far: 0
[17:02:28] compactor cycles observed so far: 0
[17:02:38] compactor cycles observed so far: 0
[17:02:48] compactor cycles observed so far: 1
[17:02:58] compactor cycles observed so far: 1
[17:03:08] compactor cycles observed so far: 1
[17:03:18] compactor cycles observed so far: 1
[17:03:18] compactor appears settled (30s without new cycles)
[17:03:23] phase 4: benchmarking compacted dataset
[+]  4/4t 4/43
 ✔ Container e2e-minio-1       Healthy                                                                       0.5s
 ✔ Container e2e-writer-1      Healthy                                                                       1.6s
 ✔ Container e2e-querier-1     Running                                                                       0.0s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.1s
Container e2e-querier-1 Waiting
Container e2e-querier-1 Healthy
Container e2e-bench-run-a6745b034785 Creating
Container e2e-bench-run-a6745b034785 Created
benchmarking 4 queries × 5 runs (tag=compacted)
  full_scan: warming...
^CTraceback (most recent call last):
  File "/work/benchmark.py", line 107, in <module>
    sys.exit(main())
             ^^^^^^
  File "/work/benchmark.py", line 77, in main
    warm_up(sql)
  File "/work/benchmark.py", line 46, in warm_up
    run_query(sql)
  File "/work/benchmark.py", line 34, in run_query
    body = resp.read()
           ^^^^^^^^^^^
  File "/usr/local/lib/python3.12/http/client.py", line 478, in read
    return self._read_chunked(amt)
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/lib/python3.12/http/client.py", line 602, in _read_chunked
    while (chunk_left := self._get_chunk_left()) is not None:
                         ^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/lib/python3.12/http/client.py", line 584, in _get_chunk_left
    chunk_left = self._read_next_chunk_size()
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/lib/python3.12/http/client.py", line 544, in _read_next_chunk_size
    line = self.fp.readline(_MAXLINE + 1)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/local/lib/python3.12/socket.py", line 720, in readinto
    return self._sock.recv_into(b)
           ^^^^^^^^^^^^^^^^^^^^^^^
KeyboardInterrupt

[17:04:30] capturing container logs
[17:04:30] tearing down stack
[+] down 7/7
 ✔ Container e2e-querier-1     Removed                                                                       0.3s
 ✔ Container e2e-compactor-1   Removed                                                                       0.1s
 ✔ Container e2e-writer-1      Removed                                                                       0.2s
 ✔ Container e2e-minio-setup-1 Removed                                                                       0.0s
 ✔ Container e2e-minio-1       Removed                                                                       0.2s
 ✔ Volume e2e_minio-data       Removed                                                                       0.1s
 ✔ Network e2e_default         Removed                                                                       0.2s
jochen.weber:e2e/ (jochen42/compactor-fix-and-modes✗) $ PARALLEL=4 WRITER_PORT=18181 QUERIER_PORT=18281 MINIO_PORT=19000 MINIO_CONSOLE_PORT=19001  HOURS=2400 HOSTS=2000 POINTS_PER_HOST=500 BATCH_SIZE=5000 ./run-bench.sh
[17:10:42] phase 0: starting writer + querier (no compactor)
[+] up 6/6
 ✔ Network e2e_default         Created                         0.0s
 ✔ Volume e2e_minio-data       Created                         0.0s
 ✔ Container e2e-minio-1       Healthy                         6.5s
 ✔ Container e2e-minio-setup-1 Exited                          6.5s
 ✔ Container e2e-writer-1      Healthy                         6.4s
 ✔ Container e2e-querier-1     Healthy                         8.4s
[17:10:51] phase 1: generating data (hours=2400 hosts=2000 points-per-host=500 batch-size=5000)
[+]  3/3t 3/32
 ✔ Container e2e-minio-1       Healthy                         0.5s
 ✔ Container e2e-writer-1      Running                         0.0s
 ✔ Container e2e-minio-setup-1 Exited                          0.6s
Container e2e-writer-1 Waiting
Container e2e-writer-1 Healthy
Container e2e-gen-run-fa5d536b8c90 Creating
Container e2e-gen-run-fa5d536b8c90 Created
plan: 2000 hosts × 500 points = 1000000 total points over 2400h (start=1770311453152302253 end=1778951453152302253) parallel=4
  progress: 100000/1000000 (10.0%) at 20440 points/s
  progress: 200000/1000000 (20.0%) at 20227 points/s
  progress: 300000/1000000 (30.0%) at 20149 points/s
  progress: 400000/1000000 (40.0%) at 20113 points/s
  progress: 500000/1000000 (50.0%) at 20070 points/s
  progress: 600000/1000000 (60.0%) at 20071 points/s
  progress: 700000/1000000 (70.0%) at 20058 points/s
  progress: 800000/1000000 (80.0%) at 20050 points/s
  progress: 900000/1000000 (90.0%) at 20051 points/s
  progress: 1000000/1000000 (100.0%) at 20052 points/s
done: wrote 1000000 points in 49.9s (20052 points/s)
[17:11:43] phase 1b: waiting for writer to flush WAL + publish inventory
[17:13:43] phase 2: benchmarking uncompacted dataset
[+]  4/4t 4/43
 ✔ Container e2e-minio-1       Healthy                                 0.5s
 ✔ Container e2e-writer-1      Healthy                                 1.6s
 ✔ Container e2e-querier-1     Running                                 0.0s
 ✔ Container e2e-minio-setup-1 Exited                                  1.1s
Container e2e-querier-1 Waiting
Container e2e-querier-1 Healthy
Container e2e-bench-run-26ba2284d52a Creating
Container e2e-bench-run-26ba2284d52a Created
benchmarking 4 queries × 5 runs (tag=uncompacted)
  full_scan: warming...
    run 1/5: 3.078s (184 bytes)
    run 2/5: 3.116s (185 bytes)
    run 3/5: 3.146s (185 bytes)
    run 4/5: 3.108s (185 bytes)
    run 5/5: 3.146s (185 bytes)
  high_card_filter: warming...
    run 1/5: 0.662s (1436 bytes)
    run 2/5: 0.705s (1436 bytes)
    run 3/5: 0.688s (1436 bytes)
    run 4/5: 0.676s (1436 bytes)
    run 5/5: 0.632s (1436 bytes)
  hour_buckets: warming...
    run 1/5: 1.306s (55817 bytes)
    run 2/5: 1.302s (55897 bytes)
    run 3/5: 1.297s (55817 bytes)
    run 4/5: 1.296s (55907 bytes)
    run 5/5: 1.323s (55897 bytes)
  narrow_window: warming...
    run 1/5: 0.080s (167 bytes)
    run 2/5: 0.090s (160 bytes)
    run 3/5: 0.093s (160 bytes)
    run 4/5: 0.083s (168 bytes)
    run 5/5: 0.085s (168 bytes)
wrote results/uncompacted.json
[17:14:17] uncompacted parquet object count (best effort):        0
unknown
[17:14:17] phase 3: starting compactor; will poll for settle
[+] up 3/3
 ✔ Container e2e-minio-1       Healthy                                 1.7s
 ✔ Container e2e-compactor-1   Healthy                                 1.7s
 ✔ Container e2e-minio-setup-1 Exited                                  1.1s
[17:14:18] compactor cycles observed so far: 0
[17:14:29] compactor cycles observed so far: 0
[17:14:39] compactor cycles observed so far: 0
[17:14:49] compactor cycles observed so far: 0
[17:14:59] compactor cycles observed so far: 1
[17:15:09] compactor cycles observed so far: 1
[17:15:19] compactor cycles observed so far: 1
[17:15:29] compactor cycles observed so far: 1
[17:15:29] compactor appears settled (30s without new cycles)
[17:15:34] phase 4: benchmarking compacted dataset
[+]  4/4t 4/43
 ✔ Container e2e-minio-1       Healthy                                 0.5s
 ✔ Container e2e-writer-1      Healthy                                 1.6s
 ✔ Container e2e-querier-1     Running                                 0.0s
 ✔ Container e2e-minio-setup-1 Exited                                  1.1s
Container e2e-querier-1 Waiting
Container e2e-querier-1 Healthy
Container e2e-bench-run-6b1983d990c3 Creating
Container e2e-bench-run-6b1983d990c3 Created
benchmarking 4 queries × 5 runs (tag=compacted)
  full_scan: warming...
    run 1/5: 1.690s (184 bytes)
    run 2/5: 1.705s (184 bytes)
    run 3/5: 1.769s (184 bytes)
    run 4/5: 1.746s (187 bytes)
    run 5/5: 1.703s (185 bytes)
  high_card_filter: warming...
    run 1/5: 0.224s (1436 bytes)
    run 2/5: 0.238s (1436 bytes)
    run 3/5: 0.209s (1436 bytes)
    run 4/5: 0.223s (1436 bytes)
    run 5/5: 0.221s (1436 bytes)
  hour_buckets: warming...
    run 1/5: 0.686s (55942 bytes)
    run 2/5: 0.687s (55977 bytes)
    run 3/5: 0.685s (55924 bytes)
    run 4/5: 0.679s (55977 bytes)
    run 5/5: 0.696s (55977 bytes)
  narrow_window: warming...
    run 1/5: 0.041s (168 bytes)
    run 2/5: 0.041s (168 bytes)
    run 3/5: 0.044s (168 bytes)
    run 4/5: 0.040s (168 bytes)
    run 5/5: 0.040s (168 bytes)
wrote results/compacted.json
[17:15:53] phase 5: rendering comparison

query                     uncompacted (s)    compacted (s)    speedup
----------------------------------------------------------------------
full_scan                           3.116            1.705      1.83x
high_card_filter                    0.676            0.223      3.03x
hour_buckets                        1.302            0.686      1.90x
narrow_window                       0.085            0.041      2.07x

runs per query: 5
[17:15:53] done — full results in e2e/results/{uncompacted,compacted}.json
[17:15:53] capturing container logs
[17:15:53] tearing down stack
[+] down 7/7
 ✔ Container e2e-compactor-1   Removed                                 0.3s
 ✔ Container e2e-querier-1     Removed                                 0.2s
 ✔ Container e2e-writer-1      Removed                                 0.2s
 ✔ Container e2e-minio-setup-1 Removed                                 0.0s
 ✔ Container e2e-minio-1       Removed                                 0.2s
 ✔ Network e2e_default         Removed                                 0.2s
 ✔ Volume e2e_minio-data       Removed                                 0.0s
 ```
