# tigerbeetle-benchmarks

Experimenting with [1000x Faster Financial Transaction Database](https://tigerbeetle.com/) built for the next 30 years. Vibe coded.

## Results

```
goos: darwin
goarch: arm64
pkg: kizzx2.com/tigerbeetle-benchmarks
cpu: Apple M4 Pro
```

| Benchmark Name        | Time (ns/op) |
|-|-|
|BenchmarkBasicTigerBeetle | 26765000 ns/op|
|BenchmarkBasicRedis | 998173 ns/op|
|BenchmarkBasicPostgres | 1663902 ns/op|
|BenchmarkTwoPhaseTigerBeetle | 77374708 ns/op|
|BenchmarkTwoPhaseRedis | 973298 ns/op|
|BenchmarkTwoPhasePostgres | 2768751 ns/op|
|BenchmarkBasicBatchTigerBeetle | 62640641 ns/op|
|BenchmarkBasicBatchRedis | 3160213 ns/op|
|BenchmarkBasicBatchPostgres | 490459944 ns/op|

```
PASS
ok      kizzx2.com/tigerbeetle-benchmarks       208.601s
```

## TL;DR

- TigerBeetle is 26x slower than Redis and 16x slower than Postgres for basic transactions.
- TigerBeetle is 79x slower than Redis and 28x slower than Postgres for two-phase transactions.
- TigerBeetle is 20x slower than Redis and 10x faster than Postgres for batches of 1000 transactions.

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