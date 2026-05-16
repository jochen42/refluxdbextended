
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

`8e5d8cf4d2250e96c259f2873ea4c5875a887cd2`

## Command

```bash
PARALLEL=4 WRITER_PORT=18181 QUERIER_PORT=18281 MINIO_PORT=19000 MINIO_CONSOLE_PORT=19001  HOURS=2400 HOSTS=2000 POINTS_PER_HOST=500 BATCH_SIZE=5000 ./run-bench.sh
[14:54:40] phase 0: starting writer + querier (no compactor)
[+] up 6/6
 ✔ Network e2e_default         Created                                                                       0.0s
 ✔ Volume e2e_minio-data       Created                                                                       0.0s
 ✔ Container e2e-minio-1       Healthy                                                                       6.5s
 ✔ Container e2e-minio-setup-1 Exited                                                                        6.4s
 ✔ Container e2e-writer-1      Healthy                                                                       6.4s
 ✔ Container e2e-querier-1     Healthy                                                                       8.4s
[14:54:48] phase 1: generating data (hours=2400 hosts=2000 points-per-host=500 batch-size=5000)
[+]  3/3t 3/32
 ✔ Container e2e-minio-1       Healthy                                                                       0.5s
 ✔ Container e2e-writer-1      Running                                                                       0.0s
 ✔ Container e2e-minio-setup-1 Exited                                                                        0.6s
Container e2e-writer-1 Waiting
Container e2e-writer-1 Healthy
Container e2e-gen-run-90e383baab73 Creating
Container e2e-gen-run-90e383baab73 Created
plan: 2000 hosts × 500 points = 1000000 total points over 2400h (start=1770303290826118761 end=1778943290826118761) parallel=4
  progress: 100000/1000000 (10.0%) at 20431 points/s
  progress: 200000/1000000 (20.0%) at 20289 points/s
  progress: 300000/1000000 (30.0%) at 20149 points/s
  progress: 400000/1000000 (40.0%) at 20134 points/s
  progress: 500000/1000000 (50.0%) at 20089 points/s
  progress: 600000/1000000 (60.0%) at 20091 points/s
  progress: 700000/1000000 (70.0%) at 20071 points/s
  progress: 800000/1000000 (80.0%) at 20065 points/s
  progress: 900000/1000000 (90.0%) at 20060 points/s
  progress: 1000000/1000000 (100.0%) at 20060 points/s
done: wrote 1000000 points in 49.9s (20060 points/s)
[14:55:40] phase 1b: waiting for writer to flush WAL + publish inventory
[14:57:40] phase 1c: restarting querier to pick up new catalog + inventory
[+] restart 0/1
 ⠸ Container e2e-querier-1 Restarting                                                                        0.3s
[+] up 4/4
 ✔ Container e2e-minio-1       Healthy                                                                       2.1s
 ✔ Container e2e-writer-1      Healthy                                                                       2.1s
 ✔ Container e2e-querier-1     Healthy                                                                       2.1s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.5s
[14:57:43] phase 2: benchmarking uncompacted dataset
[+]  4/4t 4/43
 ✔ Container e2e-minio-1       Healthy                                                                       0.5s
 ✔ Container e2e-writer-1      Healthy                                                                       1.6s
 ✔ Container e2e-querier-1     Running                                                                       0.0s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.1s
Container e2e-querier-1 Waiting
Container e2e-querier-1 Healthy
Container e2e-bench-run-8fa0685d6cdf Creating
Container e2e-bench-run-8fa0685d6cdf Created
benchmarking 4 queries × 15 runs (tag=uncompacted)
  full_scan: warming...
    run 1/15: 2.035s (185 bytes)
    run 2/15: 1.996s (187 bytes)
    run 3/15: 1.998s (185 bytes)
    run 4/15: 1.976s (185 bytes)
    run 5/15: 1.992s (187 bytes)
    run 6/15: 2.040s (184 bytes)
    run 7/15: 2.072s (186 bytes)
    run 8/15: 2.052s (186 bytes)
    run 9/15: 1.991s (186 bytes)
    run 10/15: 2.036s (186 bytes)
    run 11/15: 2.081s (185 bytes)
    run 12/15: 2.072s (184 bytes)
    run 13/15: 2.035s (186 bytes)
    run 14/15: 2.037s (185 bytes)
    run 15/15: 2.046s (185 bytes)
  high_card_filter: warming...
    run 1/15: 0.393s (1435 bytes)
    run 2/15: 0.424s (1435 bytes)
    run 3/15: 0.401s (1435 bytes)
    run 4/15: 0.422s (1435 bytes)
    run 5/15: 0.448s (1435 bytes)
    run 6/15: 0.400s (1435 bytes)
    run 7/15: 0.407s (1435 bytes)
    run 8/15: 0.427s (1435 bytes)
    run 9/15: 0.379s (1435 bytes)
    run 10/15: 0.420s (1435 bytes)
    run 11/15: 0.400s (1435 bytes)
    run 12/15: 0.418s (1435 bytes)
    run 13/15: 0.431s (1435 bytes)
    run 14/15: 0.421s (1435 bytes)
    run 15/15: 0.452s (1435 bytes)
  hour_buckets: warming...
    run 1/15: 0.802s (57270 bytes)
    run 2/15: 0.781s (57270 bytes)
    run 3/15: 0.774s (57270 bytes)
    run 4/15: 0.783s (57270 bytes)
    run 5/15: 0.805s (57290 bytes)
    run 6/15: 0.805s (57296 bytes)
    run 7/15: 0.820s (57270 bytes)
    run 8/15: 0.778s (57270 bytes)
    run 9/15: 0.812s (57270 bytes)
    run 10/15: 0.764s (57270 bytes)
    run 11/15: 0.784s (57270 bytes)
    run 12/15: 0.814s (57270 bytes)
    run 13/15: 0.774s (57270 bytes)
    run 14/15: 0.766s (57290 bytes)
    run 15/15: 0.780s (57270 bytes)
  narrow_window: warming...
    run 1/15: 0.076s (167 bytes)
    run 2/15: 0.063s (165 bytes)
    run 3/15: 0.067s (166 bytes)
    run 4/15: 0.062s (165 bytes)
    run 5/15: 0.065s (168 bytes)
    run 6/15: 0.083s (164 bytes)
    run 7/15: 0.061s (168 bytes)
    run 8/15: 0.071s (164 bytes)
    run 9/15: 0.059s (167 bytes)
    run 10/15: 0.066s (166 bytes)
    run 11/15: 0.057s (169 bytes)
    run 12/15: 0.056s (166 bytes)
    run 13/15: 0.059s (166 bytes)
    run 14/15: 0.058s (167 bytes)
    run 15/15: 0.089s (168 bytes)
wrote results/uncompacted.json
[14:58:39] uncompacted parquet object count (best effort):        0
unknown
[14:58:39] phase 3: starting compactor; will poll for settle
[+] up 3/3
 ✔ Container e2e-minio-1       Healthy                                                                       1.7s
 ✔ Container e2e-compactor-1   Healthy                                                                       1.7s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.1s
[14:58:40] compactor cycles observed so far: 0
[14:58:51] compactor cycles observed so far: 0
[14:59:01] compactor cycles observed so far: 0
[14:59:11] compactor cycles observed so far: 0
[14:59:21] compactor cycles observed so far: 1
[14:59:31] compactor cycles observed so far: 1
[14:59:41] compactor cycles observed so far: 1
[14:59:51] compactor cycles observed so far: 1
[14:59:51] compactor appears settled (30s without new cycles)
[14:59:51] phase 3b: restarting querier to pick up compaction manifests
[+] restart 0/1
 ⠸ Container e2e-querier-1 Restarting                                                                        0.3s
[+] up 4/4
 ✔ Container e2e-minio-1       Healthy                                                                       2.1s
 ✔ Container e2e-writer-1      Healthy                                                                       2.1s
 ✔ Container e2e-querier-1     Healthy                                                                       2.1s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.5s
[14:59:54] phase 4: benchmarking compacted dataset
[+]  4/4t 4/43
 ✔ Container e2e-minio-1       Healthy                                                                       0.5s
 ✔ Container e2e-writer-1      Healthy                                                                       1.6s
 ✔ Container e2e-querier-1     Running                                                                       0.0s
 ✔ Container e2e-minio-setup-1 Exited                                                                        1.1s
Container e2e-querier-1 Waiting
Container e2e-querier-1 Healthy
Container e2e-bench-run-ac516cd80f6b Creating
Container e2e-bench-run-ac516cd80f6b Created
benchmarking 4 queries × 15 runs (tag=compacted)
  full_scan: warming...
    run 1/15: 0.017s (185 bytes)
    run 2/15: 0.016s (183 bytes)
    run 3/15: 0.017s (185 bytes)
    run 4/15: 0.016s (184 bytes)
    run 5/15: 0.016s (185 bytes)
    run 6/15: 0.016s (185 bytes)
    run 7/15: 0.016s (186 bytes)
    run 8/15: 0.016s (185 bytes)
    run 9/15: 0.016s (186 bytes)
    run 10/15: 0.016s (184 bytes)
    run 11/15: 0.016s (184 bytes)
    run 12/15: 0.016s (184 bytes)
    run 13/15: 0.016s (185 bytes)
    run 14/15: 0.017s (185 bytes)
    run 15/15: 0.017s (186 bytes)
  high_card_filter: warming...
    run 1/15: 0.039s (1435 bytes)
    run 2/15: 0.032s (1435 bytes)
    run 3/15: 0.033s (1435 bytes)
    run 4/15: 0.031s (1435 bytes)
    run 5/15: 0.031s (1435 bytes)
    run 6/15: 0.031s (1435 bytes)
    run 7/15: 0.030s (1435 bytes)
    run 8/15: 0.030s (1435 bytes)
    run 9/15: 0.031s (1435 bytes)
    run 10/15: 0.031s (1435 bytes)
    run 11/15: 0.031s (1435 bytes)
    run 12/15: 0.031s (1435 bytes)
    run 13/15: 0.031s (1435 bytes)
    run 14/15: 0.031s (1435 bytes)
    run 15/15: 0.030s (1435 bytes)
  hour_buckets: warming...
    run 1/15: 0.046s (57306 bytes)
    run 2/15: 0.046s (57283 bytes)
    run 3/15: 0.046s (57386 bytes)
    run 4/15: 0.046s (57323 bytes)
    run 5/15: 0.047s (57323 bytes)
    run 6/15: 0.045s (57386 bytes)
    run 7/15: 0.046s (57285 bytes)
    run 8/15: 0.047s (57300 bytes)
    run 9/15: 0.046s (57255 bytes)
    run 10/15: 0.047s (57304 bytes)
    run 11/15: 0.047s (57310 bytes)
    run 12/15: 0.046s (57306 bytes)
    run 13/15: 0.046s (57294 bytes)
    run 14/15: 0.047s (57276 bytes)
    run 15/15: 0.049s (57301 bytes)
  narrow_window: warming...
    run 1/15: 0.028s (165 bytes)
    run 2/15: 0.026s (165 bytes)
    run 3/15: 0.026s (165 bytes)
    run 4/15: 0.026s (165 bytes)
    run 5/15: 0.026s (165 bytes)
    run 6/15: 0.026s (165 bytes)
    run 7/15: 0.026s (165 bytes)
    run 8/15: 0.026s (165 bytes)
    run 9/15: 0.026s (166 bytes)
    run 10/15: 0.026s (166 bytes)
    run 11/15: 0.026s (167 bytes)
    run 12/15: 0.026s (165 bytes)
    run 13/15: 0.026s (165 bytes)
    run 14/15: 0.026s (165 bytes)
    run 15/15: 0.026s (165 bytes)
wrote results/compacted.json
[14:59:58] phase 5: rendering comparison

query                     uncompacted (s)    compacted (s)    speedup
----------------------------------------------------------------------
full_scan                           2.036            0.016    126.40x
high_card_filter                    0.420            0.031     13.65x
hour_buckets                        0.783            0.046     16.89x
narrow_window                       0.063            0.026      2.42x

runs per query: 15
[14:59:58] done — full results in e2e/results/{uncompacted,compacted}.json
[14:59:58] capturing container logs
[14:59:59] tearing down stack
[+] down 7/7
 ✔ Container e2e-querier-1     Removed                                                                       0.2s
 ✔ Container e2e-compactor-1   Removed                                                                       0.2s
 ✔ Container e2e-writer-1      Removed                                                                       0.2s
 ✔ Container e2e-minio-setup-1 Removed                                                                       0.0s
 ✔ Container e2e-minio-1       Removed                                                                       0.2s
 ✔ Volume e2e_minio-data       Removed                                                                       0.0s
 ✔ Network e2e_default         Removed
```
