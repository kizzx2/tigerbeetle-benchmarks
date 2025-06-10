package main

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func assertParallel(tb testing.TB, a, b interface{}, field string) {
	tb.Helper()
	if fmt.Sprintf("%v", a) != fmt.Sprintf("%v", b) { // Compare string representations for simplicity with atomic types
		tb.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, b, b, a, a)
	}
}

var nextTxIDParallel uint64 = 1000000
var tigerBeetleTotalTransferredParallel uint64 = 0

func BenchmarkBasicTigerBeetleParallel(b *testing.B) {
	client, err := NewClient(ToUint128(0), []string{"127.0.0.1:3000"})
	if err != nil {
		b.Fatalf("Failed to create TigerBeetle client: %v", err)
	}
	defer client.Close()

	var accountID1 uint64 = 101 // Use different account IDs to avoid conflicts if run in same environment
	var accountID2 uint64 = 102

	_, err = client.CreateAccounts([]Account{
		{
			ID:     ToUint128(accountID1),
			Ledger: 1,
			Code:   1,
		},
		{
			ID:     ToUint128(accountID2),
			Ledger: 1,
			Code:   1,
		},
	})
	if err != nil {
		b.Fatalf("Could not create accounts: %s", err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			currentTxID := atomic.AddUint64(&nextTxIDParallel, 1)
			transferRes, err := client.CreateTransfers([]Transfer{
				{
					ID:              ToUint128(currentTxID),
					DebitAccountID:  ToUint128(accountID1),
					CreditAccountID: ToUint128(accountID2),
					Amount:          ToUint128(10),
					Ledger:          1,
					Code:            1,
				},
			})
	
			if err != nil {
				b.Errorf("Error creating transfer: %s", err)
				continue
			}
			if len(transferRes) > 0 && transferRes[0].Result != TransferOK {
				b.Errorf("Error creating transfer: %s", transferRes[0].Result)
				continue
			}
			atomic.AddUint64(&tigerBeetleTotalTransferredParallel, 10)
		}
	})

	b.StopTimer() // Stop timer before final assertions

	finalTotalTransferred := atomic.LoadUint64(&tigerBeetleTotalTransferredParallel)

	accounts, err := client.LookupAccounts([]Uint128{ToUint128(accountID1), ToUint128(accountID2)})
	if err != nil {
		b.Fatalf("Could not fetch accounts post-benchmark: %s", err)
	}
	if len(accounts) != 2 {
		b.Fatalf("Expected 2 accounts, got %d", len(accounts))
	}

	for _, account := range accounts {
		if account.ID == ToUint128(accountID1) {
			assertParallel(b, account.DebitsPosted, ToUint128(finalTotalTransferred), "account 1 debits")
			assertParallel(b, account.CreditsPosted, ToUint128(0), "account 1 credits")
		} else if account.ID == ToUint128(accountID2) {
			assertParallel(b, account.DebitsPosted, ToUint128(0), "account 2 debits")
			assertParallel(b, account.CreditsPosted, ToUint128(finalTotalTransferred), "account 2 credits")
		} else {
			b.Fatalf("Unexpected account ID %s", account.ID.String())
		}
	}
}

var redisTotalTransferredParallel int64 = 0

func BenchmarkBasicRedisParallel(b *testing.B) {
	atomic.StoreInt64(&redisTotalTransferredParallel, 0) // Reset before benchmark
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	accountID1Redis := "account103"
	accountID2Redis := "account104"

	err := rdb.Set(ctx, accountID1Redis, 0, 0).Err()
	if err != nil {
		b.Fatalf("Failed to initialize Redis %s: %v", accountID1Redis, err)
	}
	err = rdb.Set(ctx, accountID2Redis, 0, 0).Err()
	if err != nil {
		b.Fatalf("Failed to initialize Redis %s: %v", accountID2Redis, err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pipe := rdb.TxPipeline()
			pipe.DecrBy(ctx, accountID1Redis, 10)
			pipe.IncrBy(ctx, accountID2Redis, 10)
			_, err := pipe.Exec(ctx)
			if err != nil {
				b.Errorf("Redis transaction failed: %v", err)
				continue
			}
			atomic.AddInt64(&redisTotalTransferredParallel, 10)
		}
	})

	b.StopTimer()

	finalTotalTransferredRedis := atomic.LoadInt64(&redisTotalTransferredParallel)

	val1Str, err := rdb.Get(ctx, accountID1Redis).Result()
	if err != nil {
		b.Fatalf("Failed to get Redis %s: %v", accountID1Redis, err)
	}
	val2Str, err := rdb.Get(ctx, accountID2Redis).Result()
	if err != nil {
		b.Fatalf("Failed to get Redis %s: %v", accountID2Redis, err)
	}
	val1, _ := strconv.ParseInt(val1Str, 10, 64)
	val2, _ := strconv.ParseInt(val2Str, 10, 64)

	assertParallel(b, val1, -finalTotalTransferredRedis, "redis account 1 balance")
	assertParallel(b, val2, finalTotalTransferredRedis, "redis account 2 balance")
}

var postgresTotalTransferredParallel int64 = 0

func BenchmarkBasicPostgresParallel(b *testing.B) {
	atomic.StoreInt64(&postgresTotalTransferredParallel, 0) // Reset before benchmark
	ctx := context.Background()

	connString := "postgres://postgres:postgres@127.0.0.1:5432/postgres"
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		b.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	accountID1Postgres := 103
	accountID2Postgres := 104

	// Setup table on a single connection from the pool
	conn, err := pool.Acquire(ctx)
	if err != nil {
		b.Fatalf("Unable to acquire connection for setup: %v", err)
	}
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS accounts_parallel")
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE accounts_parallel (
			id INT PRIMARY KEY,
			balance BIGINT NOT NULL
		)`))
	if err != nil {
		conn.Release()
		b.Fatalf("Failed to create table accounts_parallel: %v", err)
	}
	_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO accounts_parallel (id, balance) VALUES (%d, 0), (%d, 0)", accountID1Postgres, accountID2Postgres))
	if err != nil {
		conn.Release()
		b.Fatalf("Failed to insert initial accounts into accounts_parallel: %v", err)
	}
	conn.Release() // Release the setup connection

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Acquire a connection from the pool for each goroutine
			conn, err := pool.Acquire(ctx)
			if err != nil {
				b.Fatalf("Unable to acquire connection from pool: %v", err)
				continue
			}

			tx, err := conn.Begin(ctx)
			if err != nil {
				conn.Release() // Release connection if Begin fails
				b.Fatalf("Failed to begin transaction: %v", err)
				continue
			}

			_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE accounts_parallel SET balance = balance - 10 WHERE id = %d", accountID1Postgres))
			if err != nil {
				_ = tx.Rollback(ctx)
				conn.Release() // Release connection on error
				b.Fatalf("Failed to debit account %d: %v", accountID1Postgres, err)
				continue
			}

			_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE accounts_parallel SET balance = balance + 10 WHERE id = %d", accountID2Postgres))
			if err != nil {
				_ = tx.Rollback(ctx)
				conn.Release() // Release connection on error
				b.Fatalf("Failed to credit account %d: %v", accountID2Postgres, err)
				continue
			}

			err = tx.Commit(ctx)
			if err != nil {
				conn.Release() // Release connection on error
				b.Fatalf("Failed to commit transaction: %v", err)
				continue
			}
			conn.Release() // Release the connection back to the pool
			atomic.AddInt64(&postgresTotalTransferredParallel, 10)
		}
	})

	b.StopTimer()

	finalTotalTransferredPostgres := atomic.LoadInt64(&postgresTotalTransferredParallel)

	// Query final balances on a single connection from the pool
	queryConn, err := pool.Acquire(ctx)
	if err != nil {
		b.Fatalf("Unable to acquire connection for querying balances: %v", err)
	}
	defer queryConn.Release()

	var bal1, bal2 int64
	err = queryConn.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM accounts_parallel WHERE id = %d", accountID1Postgres)).Scan(&bal1)
	if err != nil {
		b.Fatalf("Failed to query account %d balance: %v", accountID1Postgres, err)
	}
	err = queryConn.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM accounts_parallel WHERE id = %d", accountID2Postgres)).Scan(&bal2)
	if err != nil {
		b.Fatalf("Failed to query account %d balance: %v", accountID2Postgres, err)
	}

	assertParallel(b, bal1, -finalTotalTransferredPostgres, "postgres account 1 balance")
	assertParallel(b, bal2, finalTotalTransferredPostgres, "postgres account 2 balance")
}
