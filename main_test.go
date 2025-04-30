package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v5"
	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func assert(a, b interface{}, field string) {
	if !reflect.DeepEqual(a, b) {
		log.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, b, b, a, a)
	}
}

var nextTxID uint64 = 1
var tigerBeetleTotalTransferred uint64 = 0

func BenchmarkBasicTigerBeetle(b *testing.B) {
	client, err := NewClient(ToUint128(0), []string{"127.0.0.1:3000"})
	if err != nil {
		panic(fmt.Sprintf("Failed to create TigerBeetle client: %v", err))
	}
	defer client.Close()

	var accountID1 uint64 = 1
	var accountID2 uint64 = 2

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
		panic(fmt.Sprintf("Failed to create accounts: %v", err))
	}
	// for _, err := range res {
	// 	log.Fatalf("Error creating account %d: %s", err.Index, err.Result)
	// }

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transferRes, err := client.CreateTransfers([]Transfer{
			{
				ID:              ToUint128(nextTxID),
				DebitAccountID:  ToUint128(accountID1),
				CreditAccountID: ToUint128(accountID2),
				Amount:          ToUint128(10),
				Ledger:          1,
				Code:            1,
			},
		})
		if err != nil {
			log.Fatalf("Error creating transfer: %s", err)
		}

		nextTxID++
		tigerBeetleTotalTransferred += 10

		for _, err := range transferRes {
			log.Fatalf("Error creating transfer: %s", err.Result)
		}

		accounts, err := client.LookupAccounts([]Uint128{ToUint128(accountID1), ToUint128(accountID2)})
		if err != nil {
			log.Fatalf("Could not fetch accounts: %s", err)
		}
		assert(len(accounts), 2, "accounts")

		for _, account := range accounts {
			if account.ID == ToUint128(accountID1) {
				assert(account.DebitsPosted, ToUint128(tigerBeetleTotalTransferred), "account 1 debits")
				assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
			} else if account.ID == ToUint128(accountID2) {
				assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
				assert(account.CreditsPosted, ToUint128(tigerBeetleTotalTransferred), "account 2 credits")
			} else {
				log.Fatalf("Unexpected account")
			}
		}
	}
}

var redisTotalTransferred int64 = 0

func BenchmarkBasicRedis(b *testing.B) {
	redisTotalTransferred = 0
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	err := rdb.Set(ctx, "account1", 0, 0).Err()
	if err != nil {
		log.Fatalf("Failed to initialize Redis account1: %v", err)
	}
	err = rdb.Set(ctx, "account2", 0, 0).Err()
	if err != nil {
		log.Fatalf("Failed to initialize Redis account2: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pipe := rdb.TxPipeline()
		pipe.DecrBy(ctx, "account1", 10)
		pipe.IncrBy(ctx, "account2", 10)
		_, err := pipe.Exec(ctx)
		if err != nil {
			log.Fatalf("Redis transaction failed: %v", err)
		}

		redisTotalTransferred += 10

		val1Str, err := rdb.Get(ctx, "account1").Result()
		if err != nil {
			log.Fatalf("Failed to get Redis account1: %v", err)
		}
		val2Str, err := rdb.Get(ctx, "account2").Result()
		if err != nil {
			log.Fatalf("Failed to get Redis account2: %v", err)
		}
		val1, _ := strconv.ParseInt(val1Str, 10, 64)
		val2, _ := strconv.ParseInt(val2Str, 10, 64)

		assert(val1, -redisTotalTransferred, "redis account 1 balance")
		assert(val2, redisTotalTransferred, "redis account 2 balance")
	}
}

var postgresTotalTransferred int64 = 0

func BenchmarkBasicPostgres(b *testing.B) {
	postgresTotalTransferred = 0
	ctx := context.Background()

	connString := "postgres://postgres:postgres@127.0.0.1:5432/postgres"
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer conn.Close(ctx)

	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS accounts")
	_, err = conn.Exec(ctx, `
		CREATE TABLE accounts (
			id INT PRIMARY KEY,
			balance BIGINT NOT NULL
		)`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 0), (2, 0)")
	if err != nil {
		log.Fatalf("Failed to insert initial accounts: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			log.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = 1")
		if err != nil {
			_ = tx.Rollback(ctx)
			log.Fatalf("Failed to debit account 1: %v", err)
		}

		_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + 10 WHERE id = 2")
		if err != nil {
			_ = tx.Rollback(ctx)
			log.Fatalf("Failed to credit account 2: %v", err)
		}

		err = tx.Commit(ctx)
		if err != nil {
			log.Fatalf("Failed to commit transaction: %v", err)
		}

		postgresTotalTransferred += 10

		var bal1, bal2 int64
		err = conn.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = 1").Scan(&bal1)
		if err != nil {
			log.Fatalf("Failed to query account 1 balance: %v", err)
		}
		err = conn.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = 2").Scan(&bal2)
		if err != nil {
			log.Fatalf("Failed to query account 2 balance: %v", err)
		}

		assert(bal1, -postgresTotalTransferred, "postgres account 1 balance")
		assert(bal2, postgresTotalTransferred, "postgres account 2 balance")
	}
}

var tigerBeetleTotalTransferredPending uint64 = 0
var tigerBeetleTotalTransferredPosted uint64 = 0

func BenchmarkTwoPhaseTigerBeetle(b *testing.B) {
	client, err := NewClient(ToUint128(0), []string{"127.0.0.1:3000"})
	if err != nil {
		log.Fatalf("Failed to create TigerBeetle client: %s", err)
	}
	defer client.Close()

	var accountID1 uint64 = 3
	var accountID2 uint64 = 4

	// Create two accounts
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
		log.Fatalf("Error creating accounts: %s", err)
	}

	// for _, err := range res {
	// 	log.Fatalf("Error creating account %d: %s", err.Index, err.Result)
	// }

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transferRes, err := client.CreateTransfers([]Transfer{
			{
				ID:              ToUint128(nextTxID),
				DebitAccountID:  ToUint128(accountID1),
				CreditAccountID: ToUint128(accountID2),
				Amount:          ToUint128(500),
				Ledger:          1,
				Code:            1,
				Flags:           TransferFlags{Pending: true}.ToUint16(),
			},
		})
		if err != nil {
			log.Fatalf("Error creating transfer: %s", err)
		}
		pendingID := nextTxID
		nextTxID++
		tigerBeetleTotalTransferredPending += 500

		for _, err := range transferRes {
			log.Fatalf("Error creating transfer: %s", err.Result)
		}

		accounts, err := client.LookupAccounts([]Uint128{ToUint128(accountID1), ToUint128(accountID2)})
		if err != nil {
			log.Fatalf("Could not fetch accounts: %s", err)
		}
		assert(len(accounts), 2, "accounts")

		for _, account := range accounts {
			if account.ID == ToUint128(accountID1) {
				assert(account.DebitsPosted, ToUint128(tigerBeetleTotalTransferredPosted), "account 1 debits, before posted")
				assert(account.CreditsPosted, ToUint128(0), "account 1 credits, before posted")
				assert(account.DebitsPending, ToUint128(tigerBeetleTotalTransferredPending), "account 1 debits pending, before posted")
				assert(account.CreditsPending, ToUint128(0), "account 1 credits pending, before posted")
			} else if account.ID == ToUint128(accountID2) {
				assert(account.DebitsPosted, ToUint128(0), "account 2 debits, before posted")
				assert(account.CreditsPosted, ToUint128(tigerBeetleTotalTransferredPosted), "account 2 credits, before posted")
				assert(account.DebitsPending, ToUint128(0), "account 2 debits pending, before posted")
				assert(account.CreditsPending, ToUint128(tigerBeetleTotalTransferredPending), "account 2 credits pending, before posted")
			} else {
				log.Fatalf("Unexpected account: %s", account.ID)
			}
		}

		transferRes, err = client.CreateTransfers([]Transfer{
			{
				ID:              ToUint128(nextTxID),
				DebitAccountID:  ToUint128(accountID1),
				CreditAccountID: ToUint128(accountID2),
				Amount:          ToUint128(500),
				PendingID:       ToUint128(pendingID),
				Ledger:          1,
				Code:            1,
				Flags:           TransferFlags{PostPendingTransfer: true}.ToUint16(),
			},
		})
		if err != nil {
			log.Fatalf("Error creating transfers: %s", err)
		}
		postID := nextTxID
		nextTxID++

		tigerBeetleTotalTransferredPending -= 500
		tigerBeetleTotalTransferredPosted += 500

		for _, err := range transferRes {
			log.Fatalf("Error creating transfer: %s", err.Result)
		}

		transfers, err := client.LookupTransfers([]Uint128{ToUint128(pendingID), ToUint128(postID)})
		if err != nil {
			log.Fatalf("Error looking up transfers: %s", err)
		}
		assert(len(transfers), 2, "transfers")

		for _, transfer := range transfers {
			if transfer.ID == ToUint128(pendingID) {
				assert(transfer.TransferFlags().Pending, true, "transfer 1 pending")
				assert(transfer.TransferFlags().PostPendingTransfer, false, "transfer 1 post_pending_transfer")
			} else if transfer.ID == ToUint128(postID) {
				assert(transfer.TransferFlags().Pending, false, "transfer 2 pending")
				assert(transfer.TransferFlags().PostPendingTransfer, true, "transfer 2 post_pending_transfer")
			} else {
				log.Fatalf("Unknown transfer: %s", transfer.ID)
			}
		}

		accounts, err = client.LookupAccounts([]Uint128{ToUint128(accountID1), ToUint128(accountID2)})
		if err != nil {
			log.Fatalf("Could not fetch accounts: %s", err)
		}
		assert(len(accounts), 2, "accounts")

		for _, account := range accounts {
			if account.ID == ToUint128(accountID1) {
				assert(account.DebitsPosted, ToUint128(tigerBeetleTotalTransferredPosted), "account 1 debits")
				assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
				assert(account.DebitsPending, ToUint128(tigerBeetleTotalTransferredPending), "account 1 debits pending")
				assert(account.CreditsPending, ToUint128(0), "account 1 credits pending")
			} else if account.ID == ToUint128(accountID2) {
				assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
				assert(account.CreditsPosted, ToUint128(tigerBeetleTotalTransferredPosted), "account 2 credits")
				assert(account.DebitsPending, ToUint128(0), "account 2 debits pending")
				assert(account.CreditsPending, ToUint128(tigerBeetleTotalTransferredPending), "account 2 credits pending")
			} else {
				log.Fatalf("Unexpected account: %s", account.ID)
			}
		}
	}
}

var redisTotalTransferredPending int64 = 0
var redisTotalTransferredPosted int64 = 0

func BenchmarkTwoPhaseRedis(b *testing.B) {
    redisTotalTransferredPending = 0
    redisTotalTransferredPosted = 0
    ctx := context.Background()

    rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
    defer rdb.Close()

    account3ID := "account3"
    account4ID := "account4"
    keys := []string{
        account3ID + "_pending_debits", account3ID + "_pending_credits",
        account3ID + "_posted_debits", account3ID + "_posted_credits",
        account4ID + "_pending_debits", account4ID + "_pending_credits",
        account4ID + "_posted_debits", account4ID + "_posted_credits",
    }
    for _, key := range keys {
        err := rdb.Set(ctx, key, 0, 0).Err()
        if err != nil {
            log.Fatalf("Failed to initialize Redis key %s: %v", key, err)
        }
    }

    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        pipe := rdb.TxPipeline()
        pipe.IncrBy(ctx, account3ID+"_pending_debits", 500)
        pipe.IncrBy(ctx, account4ID+"_pending_credits", 500)
        _, err := pipe.Exec(ctx)
        if err != nil {
            log.Fatalf("Redis phase 1 transaction failed: %v", err)
        }
        redisTotalTransferredPending += 500

        pipe = rdb.TxPipeline()
        pipe.DecrBy(ctx, account3ID+"_pending_debits", 500)
        pipe.DecrBy(ctx, account4ID+"_pending_credits", 500)
        pipe.IncrBy(ctx, account3ID+"_posted_debits", 500)
        pipe.IncrBy(ctx, account4ID+"_posted_credits", 500)
        _, err = pipe.Exec(ctx)
        if err != nil {
            log.Fatalf("Redis phase 2 transaction failed: %v", err)
        }
        redisTotalTransferredPending -= 500
        redisTotalTransferredPosted += 500

        vals, err := rdb.MGet(ctx, keys...).Result()
        if err != nil {
            log.Fatalf("Failed to get Redis keys: %v", err)
        }

        parseInt := func(idx int) int64 {
            if idx >= len(vals) || vals[idx] == nil {
                return 0
            }
            valStr, ok := vals[idx].(string)
            if !ok {
                return 0
            }
            v, _ := strconv.ParseInt(valStr, 10, 64)
            return v
        }

        assert(parseInt(0), redisTotalTransferredPending, "redis account 3 pending debits")
        assert(parseInt(1), int64(0), "redis account 3 pending credits")
        assert(parseInt(2), redisTotalTransferredPosted, "redis account 3 posted debits")
        assert(parseInt(3), int64(0), "redis account 3 posted credits")

        assert(parseInt(4), int64(0), "redis account 4 pending debits")
        assert(parseInt(5), redisTotalTransferredPending, "redis account 4 pending credits")
        assert(parseInt(6), int64(0), "redis account 4 posted debits")
        assert(parseInt(7), redisTotalTransferredPosted, "redis account 4 posted credits")
    }
}

var postgresTotalTransferredPending int64 = 0
var postgresTotalTransferredPosted int64 = 0

func BenchmarkTwoPhasePostgres(b *testing.B) {
    postgresTotalTransferredPending = 0
    postgresTotalTransferredPosted = 0
    ctx := context.Background()

    connString := "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    conn, err := pgx.Connect(ctx, connString)
    if err != nil {
        log.Fatalf("Unable to connect to database: %v", err)
    }
    defer conn.Close(ctx)

    _, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS accounts_two_phase")
    _, err = conn.Exec(ctx, `
        CREATE TABLE accounts_two_phase (
            id INT PRIMARY KEY,
            pending_debits BIGINT NOT NULL DEFAULT 0,
            pending_credits BIGINT NOT NULL DEFAULT 0,
            posted_debits BIGINT NOT NULL DEFAULT 0,
            posted_credits BIGINT NOT NULL DEFAULT 0
        )`)
    if err != nil {
        log.Fatalf("Failed to create table accounts_two_phase: %v", err)
    }
    _, err = conn.Exec(ctx, "INSERT INTO accounts_two_phase (id) VALUES (3), (4)")
    if err != nil {
        log.Fatalf("Failed to insert initial accounts: %v", err)
    }

    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        tx1, err := conn.Begin(ctx)
        if err != nil {
            log.Fatalf("Failed to begin phase 1 transaction: %v", err)
        }
        _, err = tx1.Exec(ctx, "UPDATE accounts_two_phase SET pending_debits = pending_debits + 500 WHERE id = 3")
        if err != nil {
            _ = tx1.Rollback(ctx)
            log.Fatalf("Failed to update pending debits for account 3: %v", err)
        }
        _, err = tx1.Exec(ctx, "UPDATE accounts_two_phase SET pending_credits = pending_credits + 500 WHERE id = 4")
        if err != nil {
            _ = tx1.Rollback(ctx)
            log.Fatalf("Failed to update pending credits for account 4: %v", err)
        }
        err = tx1.Commit(ctx)
        if err != nil {
            log.Fatalf("Failed to commit phase 1 transaction: %v", err)
        }
        postgresTotalTransferredPending += 500

        tx2, err := conn.Begin(ctx)
        if err != nil {
            log.Fatalf("Failed to begin phase 2 transaction: %v", err)
        }
        _, err = tx2.Exec(ctx, "UPDATE accounts_two_phase SET pending_debits = pending_debits - 500, posted_debits = posted_debits + 500 WHERE id = 3")
        if err != nil {
            _ = tx2.Rollback(ctx)
            log.Fatalf("Failed to post debit for account 3: %v", err)
        }
        _, err = tx2.Exec(ctx, "UPDATE accounts_two_phase SET pending_credits = pending_credits - 500, posted_credits = posted_credits + 500 WHERE id = 4")
        if err != nil {
            _ = tx2.Rollback(ctx)
            log.Fatalf("Failed to post credit for account 4: %v", err)
        }
        err = tx2.Commit(ctx)
        if err != nil {
            log.Fatalf("Failed to commit phase 2 transaction: %v", err)
        }
        postgresTotalTransferredPending -= 500
        postgresTotalTransferredPosted += 500

        var pd3, pc3, psd3, psc3 int64 // pending debit/credit, posted debit/credit for account 3
        var pd4, pc4, psd4, psc4 int64 // for account 4

        err = conn.QueryRow(ctx, "SELECT pending_debits, pending_credits, posted_debits, posted_credits FROM accounts_two_phase WHERE id = 3").Scan(&pd3, &pc3, &psd3, &psc3)
        if err != nil {
            log.Fatalf("Failed to query account 3 balances: %v", err)
        }
        err = conn.QueryRow(ctx, "SELECT pending_debits, pending_credits, posted_debits, posted_credits FROM accounts_two_phase WHERE id = 4").Scan(&pd4, &pc4, &psd4, &psc4)
        if err != nil {
            log.Fatalf("Failed to query account 4 balances: %v", err)
        }

        assert(pd3, postgresTotalTransferredPending, "postgres account 3 pending debits")
        assert(pc3, int64(0), "postgres account 3 pending credits")
        assert(psd3, postgresTotalTransferredPosted, "postgres account 3 posted debits")
        assert(psc3, int64(0), "postgres account 3 posted credits")

        assert(pd4, int64(0), "postgres account 4 pending debits")
        assert(pc4, postgresTotalTransferredPending, "postgres account 4 pending credits")
        assert(psd4, int64(0), "postgres account 4 posted debits")
        assert(psc4, postgresTotalTransferredPosted, "postgres account 4 posted credits")
    }
}

var tigerBeetleTotalTransferredBatch uint64 = 0

func BenchmarkBasicBatchTigerBeetle(b *testing.B) {
	client, err := NewClient(ToUint128(0), []string{"127.0.0.1:3000"})
	if err != nil {
		panic(fmt.Sprintf("Failed to create TigerBeetle client: %v", err))
	}
	defer client.Close()

	var accountID1 uint64 = 5
	var accountID2 uint64 = 6

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
		panic(fmt.Sprintf("Failed to create accounts: %v", err))
	}
	// for _, err := range res {
	// 	log.Fatalf("Error creating account %d: %s", err.Index, err.Result)
	// }

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transfers := make([]Transfer, 1000)
		for j := 0; j < 1000; j++ {
			transfers[j] = Transfer{
				ID:              ToUint128(nextTxID),
				DebitAccountID:  ToUint128(accountID1),
				CreditAccountID: ToUint128(accountID2),
				Amount:          ToUint128(10),
				Ledger:          1,
				Code:            1,
			}
			nextTxID++
			tigerBeetleTotalTransferredBatch += 10
		}
		transferRes, err := client.CreateTransfers(transfers)
		if err != nil {
			log.Fatalf("Error creating transfer: %s", err)
		}

		for _, err := range transferRes {
			log.Fatalf("Error creating transfer: %s", err.Result)
		}

		accounts, err := client.LookupAccounts([]Uint128{ToUint128(accountID1), ToUint128(accountID2)})
		if err != nil {
			log.Fatalf("Could not fetch accounts: %s", err)
		}
		assert(len(accounts), 2, "accounts")

		for _, account := range accounts {
			if account.ID == ToUint128(accountID1) {
				assert(account.DebitsPosted, ToUint128(tigerBeetleTotalTransferredBatch), "account 1 debits")
				assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
			} else if account.ID == ToUint128(accountID2) {
				assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
				assert(account.CreditsPosted, ToUint128(tigerBeetleTotalTransferredBatch), "account 2 credits")
			} else {
				log.Fatalf("Unexpected account")
			}
		}
	}
}

var redisTotalTransferredBatch int64 = 0

func BenchmarkBasicBatchRedis(b *testing.B) {
	redisTotalTransferredBatch = 0
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	account5ID := "account5"
	account6ID := "account6"
	err := rdb.Set(ctx, account5ID, 0, 0).Err()
	if err != nil {
		log.Fatalf("Failed to initialize Redis %s: %v", account5ID, err)
	}
	err = rdb.Set(ctx, account6ID, 0, 0).Err()
	if err != nil {
		log.Fatalf("Failed to initialize Redis %s: %v", account6ID, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pipe := rdb.TxPipeline()
		for j := 0; j < 1000; j++ {
			pipe.DecrBy(ctx, account5ID, 10)
			pipe.IncrBy(ctx, account6ID, 10)
			redisTotalTransferredBatch += 10
		}
		_, err := pipe.Exec(ctx)
		if err != nil {
			log.Fatalf("Redis batch transaction failed: %v", err)
		}

		val1Str, err := rdb.Get(ctx, account5ID).Result()
		if err != nil {
			log.Fatalf("Failed to get Redis %s: %v", account5ID, err)
		}
		val2Str, err := rdb.Get(ctx, account6ID).Result()
		if err != nil {
			log.Fatalf("Failed to get Redis %s: %v", account6ID, err)
		}
		val1, _ := strconv.ParseInt(val1Str, 10, 64)
		val2, _ := strconv.ParseInt(val2Str, 10, 64)

		assert(val1, -redisTotalTransferredBatch, "redis batch account 5 balance")
		assert(val2, redisTotalTransferredBatch, "redis batch account 6 balance")
	}
}

var postgresTotalTransferredBatch int64 = 0

func BenchmarkBasicBatchPostgres(b *testing.B) {
	postgresTotalTransferredBatch = 0
	ctx := context.Background()

	connString := "postgres://postgres:postgres@127.0.0.1:5432/postgres"
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer conn.Close(ctx)

	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS accounts_batch")
	_, err = conn.Exec(ctx, `
		CREATE TABLE accounts_batch (
			id INT PRIMARY KEY,
			balance BIGINT NOT NULL
		)`)
	if err != nil {
		log.Fatalf("Failed to create table accounts_batch: %v", err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO accounts_batch (id, balance) VALUES (5, 0), (6, 0)")
	if err != nil {
		log.Fatalf("Failed to insert initial batch accounts: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			log.Fatalf("Failed to begin batch transaction: %v", err)
		}

		for j := 0; j < 1000; j++ {
			_, err = tx.Exec(ctx, "UPDATE accounts_batch SET balance = balance - 10 WHERE id = 5; UPDATE accounts_batch SET balance = balance + 10 WHERE id = 6")
			if err != nil {
				_ = tx.Rollback(ctx)
				log.Fatalf("Failed to debit batch account 5: %v", err)
			}
			postgresTotalTransferredBatch += 10
		}

		err = tx.Commit(ctx)
		if err != nil {
			log.Fatalf("Failed to commit batch transaction: %v", err)
		}

		var bal1, bal2 int64
		rows, err := conn.Query(ctx, "SELECT id, balance FROM accounts_batch WHERE id IN (5, 6) ORDER BY id")
		if err != nil {
			log.Fatalf("Failed to query batch account 5, 6 balance: %v", err)
		}
		var id int64
		rows.Next()
		if err := rows.Scan(&id, &bal1); err != nil {
			log.Fatalf("Failed to scan batch account balance: %v", err)
		}
		rows.Next()
		if err := rows.Scan(&id, &bal2); err != nil {
			log.Fatalf("Failed to scan batch account balance: %v", err)
		}
		rows.Close()

		assert(bal1, -postgresTotalTransferredBatch, "postgres batch account 5 balance")
		assert(bal2, postgresTotalTransferredBatch, "postgres batch account 6 balance")
	}
}