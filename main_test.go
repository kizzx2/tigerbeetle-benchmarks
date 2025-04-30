package main

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func assert(a, b interface{}, field string) {
	if !reflect.DeepEqual(a, b) {
		log.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, b, b, a, a)
	}
}

var nextTxID uint64 = 1
var totalTransferred uint64 = 0

func BenchmarkBasicTigerBeetle(b *testing.B) {
	client, err := NewClient(ToUint128(0), []string{"127.0.0.1:3000"})
	if err != nil {
		panic(fmt.Sprintf("Failed to create TigerBeetle client: %v", err))
	}
	defer client.Close()

	res, err := client.CreateAccounts([]Account{
		{
			ID:     ToUint128(1),
			Ledger: 1,
			Code:   1,
		},
		{
			ID:     ToUint128(2),
			Ledger: 1,
			Code:   1,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create accounts: %v", err))
	}
	for _, acc := range res {
		fmt.Printf("Created Account ID: %d", acc.Index)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transferRes, err := client.CreateTransfers([]Transfer{
			{
				ID:              ToUint128(nextTxID),
				DebitAccountID:  ToUint128(1),
				CreditAccountID: ToUint128(2),
				Amount:          ToUint128(10),
				Ledger:          1,
				Code:            1,
			},
		})
		if err != nil {
			log.Fatalf("Error creating transfer: %s", err)
		}

		nextTxID++
		totalTransferred += 10

		for _, err := range transferRes {
			log.Fatalf("Error creating transfer: %s", err.Result)
		}

		// Check the sums for both accounts
		accounts, err := client.LookupAccounts([]Uint128{ToUint128(1), ToUint128(2)})
		if err != nil {
			log.Fatalf("Could not fetch accounts: %s", err)
		}
		assert(len(accounts), 2, "accounts")

		for _, account := range accounts {
			if account.ID == ToUint128(1) {
				assert(account.DebitsPosted, ToUint128(totalTransferred), "account 1 debits")
				assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
			} else if account.ID == ToUint128(2) {
				assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
				assert(account.CreditsPosted, ToUint128(totalTransferred), "account 2 credits")
			} else {
				log.Fatalf("Unexpected account")
			}
		}
	}
}
