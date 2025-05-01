# tigerbeetle-benchmarks

Experimenting with [1000x Faster Financial Transaction Database](https://tigerbeetle.com/) built for the next 30 years. Vibe coded.

## Results

```
$ ./tigerbeetle version
TigerBeetle version 0.16.38+2371483

$ go version
go version go1.24.2 darwin/arm64

goos: darwin
goarch: arm64
pkg: kizzx2.com/tigerbeetle-benchmarks
cpu: Apple M4 Pro
```

| Benchmark Name        | Time (ns/op) |
|-|-|
| BenchmarkBasicTigerBeetle | 24362892 ns/op |
| BenchmarkBasicRedis | 173072 ns/op |
| BenchmarkBasicPostgres | 781365 ns/op |
| BenchmarkTwoPhaseTigerBeetle | 60988901 ns/op |
| BenchmarkTwoPhaseRedis | 171658 ns/op |
| BenchmarkTwoPhasePostgres | 1533958 ns/op |
| BenchmarkBasicBatchTigerBeetle | 27807630 ns/op |
| BenchmarkBasicBatchRedis | 1362045 ns/op |
| BenchmarkBasicBatchPostgres | 86402994 ns/op |

```
PASS
ok      kizzx2.com/tigerbeetle-benchmarks       204.673s
```

## TL;DR

- TigerBeetle is :red_circle: 140x slower than Redis and :red_circle: 31x slower than Postgres for basic transactions.
- TigerBeetle is :red_circle: 355x slower than Redis and :red_circle: 40x slower than Postgres for two-phase transactions.
- TigerBeetle is :red_circle: 20x slower than Redis and :large_blue_circle: 3x faster than Postgres for batches of 1000 transactions.

## Methodology

The first 2 scenarios are based on the [samples code from the tigerbeetle-go](https://github.com/tigerbeetle/tigerbeetle-go/tree/main/samples) client library.

The batch example was added based on the Basic example because it seemed to be losing too bad.

## How to Run

1. Download TigerBeetle
2. Run the following between each benchmark run:

```
rm ./0_0.tigerbeetle
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 ./0_0.tigerbeetle
./tigerbeetle start --addresses=3000 ./0_0.tigerbeetle
```

3. Start Postgres and Redis

```
docker run --rm -it -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres
docker run --rm -it -p 6379:6379 redis
```

4. Run the benchmarks

```
go test -bench=. -count=10
```

5. :popcorn: