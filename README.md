# tigerbeetle-benchmarks

Experimenting with [1000x Faster Financial Transaction Database](https://tigerbeetle.com/) built for the next 30 years. Vibe coded.

## Results

```
$ ./tigerbeetle version
TigerBeetle version 0.16.38+2371483

$ go version
go version go1.24.2 darwin/arm64 | go version go1.24.2 linux/amd64

goos: darwin | linux
goarch: arm64 | amd64
pkg: kizzx2.com/tigerbeetle-benchmarks
cpu: Apple M4 Pro | AMD Ryzen 7 5800H with Radeon Graphics         
```

| Benchmark Name | Mac M4 ns/op | Linux AMD ns/op | Mac M4 TigerBeetle faster by | Linux AMD TigerBeetle faster by |
|-|-|-|-|-|
| BenchmarkBasicTigerBeetle | 24362892 | 3763695 | - | - |
| BenchmarkBasicRedis | 173072 | 537230 | 🔴 -140x | 🔴 -6x |
| BenchmarkBasicPostgres | 781365 | 2067554 | 🔴 -30x | 🔴-1x |
| BenchmarkTwoPhaseTigerBeetle | 60988901 | 16520356 | - | - |
| BenchmarkTwoPhaseRedis | 171658 | 560849 | 🔴 -354x | 🔴 -28x |
| BenchmarkTwoPhasePostgres | 1533958 | 7032468 | 🔴 -39x | 🔴 -1x |
| BenchmarkBasicBatchTigerBeetle | 27807630 | 30229617 | - | - |
| BenchmarkBasicBatchRedis | 1362045 | 1980634 | 🔴 -20x | 🔴-14x |
| BenchmarkBasicBatchPostgres | 86402994 | 308697749 | 🔵 2x | 🔵10x |

```
PASS
ok      kizzx2.com/tigerbeetle-benchmarks       204.673s
```

## TL;DR

TigerBeetle is about 1 - 2 order of magnitudes slower than Redis / Postgres except for when batching 1000 transactions each batch, where it starts to catch up.

## Methodology

The first 2 scenarios are based on the [samples code from the tigerbeetle-go](https://github.com/tigerbeetle/tigerbeetle-go/tree/main/samples) client library.

The batch example was added based on the Basic example because it seemed to be losing too bad.

AMD benchmark was added because it was mentioned that TigerBeetle performs significantly worse on Mac than on Linux. I could confirm that on my particular Linux set up, TigerBeetle was just 100% slower than Postgres for non batch use case.

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
