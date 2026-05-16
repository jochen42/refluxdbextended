
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

97a982b4be59bac286c5d19c0bc088c5a59fd973

## Command

```bash
PARALLEL=4 WRITER_PORT=18181 QUERIER_PORT=18281 MINIO_PORT=19000 MINIO_CONSOLE_PORT=19001  HOURS=2400 HOSTS=2000 POINTS_PER_HOST=500 BATCH_SIZE=5000 ./run-bench.sh
 ```
