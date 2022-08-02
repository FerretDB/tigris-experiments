package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
)

type ObjectID [12]byte

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cfg := &config.Driver{
		URL: "localhost:8081",
	}
	dr, err := driver.NewDriver(ctx, cfg)
	if err != nil {
		panic(err)
	}

	dbName := "hello"
	colName := "users"

	// Cleanup DB before doing anything
	err = dr.DropDatabase(ctx, dbName)
	switch err := err.(type) {
	case nil:
		// do nothing
	case *driver.Error:
		if err.Code != api.Code_NOT_FOUND {
			panic(err)
		}
	default:
		panic(err)
	}

	err = dr.CreateDatabase(ctx, dbName)
	if err != nil {
		panic(err)
	}

	colParams := map[string]any{
		"title": "users",
		"type":  "object",
		"properties": map[string]any{
			"$k": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "string",
				},
			},
			"balance": map[string]any{
				"type": "number",
			},
			"_id": map[string]any{
				"type":   "string",
				"format": "byte",
			},
			"name": map[string]any{
				"type": "string",
			},
		},
		"primary_key": []any{
			"_id",
		},
	}
	cp, err := json.Marshal(colParams)
	if err != nil {
		panic(err)
	}
	err = dr.UseDatabase(dbName).CreateOrUpdateCollection(ctx, colName, cp)
	if err != nil {
		panic(err)
	}

	id := ObjectID{0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}
	mid, err := json.Marshal(id[:])

	if err != nil {
		panic(err)
	}
	userB := map[string]any{
		"name":    "Mister Infinity",
		"balance": 1,
		"_id":     mid,
	}
	res, err := json.Marshal(userB)
	if err != nil {
		panic(err)
	}

	resp, err := dr.UseDatabase(dbName).Insert(ctx, colName, []driver.Document{res})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v", resp)

	// that fails:
	//filter, err := filter.Eq("_id", id[:]).Build()

	// that works:
	filter, err := filter.Eq("_id", mid).Build()

	fmt.Printf("\n\n%v\n\n", string(filter))
	if err != nil {
		panic(err)
	}

	resp2, err := dr.UseDatabase(dbName).Delete(ctx, colName, filter)
	//resp2, err := dr.UseDatabase(dbName).Read(ctx, colName, filter, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v", resp2)
}
