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
		// if DB doesn't exist, do nothing
	case *driver.Error:
		if err.Code != api.Code_NOT_FOUND {
			panic(err)
		}
	default:
		panic(err)
	}

	// Create an empty DB
	err = dr.CreateDatabase(ctx, dbName)
	if err != nil {
		panic(err)
	}

	// Create a collection with the given schema
	colParams := map[string]any{
		"title": "users",
		"type":  "object",
		"properties": map[string]any{
			"_id": map[string]any{
				"type":   "string",
				"format": "byte",
			},
			"balance": map[string]any{
				"type": "number",
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

	// Insert a doc into collection
	id := ObjectID{0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}
	userB := map[string]any{
		"balance": 1,
		"_id":     id[:],
	}
	res, err := json.Marshal(userB)
	if err != nil {
		panic(err)
	}

	resp, err := dr.UseDatabase(dbName).Insert(ctx, colName, []driver.Document{res})
	fmt.Printf("inserted: %+v\n", resp)
	if err != nil {
		panic(err)
	}

	// Update a doc with the given filter
	filter, err := filter.Eq("_id", id[:]).Build()
	if err != nil {
		panic(err)
	}

	updateQuery := []byte(`{"balance": 5}`)
	up, err := dr.UseDatabase(dbName).Update(ctx, colName, filter, updateQuery)
	fmt.Printf("updated: %+v\n", up)
	if err != nil {
		panic(err)
	}

	/*	// Find a doc using the same filter
		found, err := dr.UseDatabase(dbName).Read(ctx, colName, filter, nil)
		fmt.Printf("found: %+v\n", found)
		if err != nil {
			panic(err)
		}

		// Delete a doc using the same filter
		del, err := dr.UseDatabase(dbName).Delete(ctx, colName, filter)
		fmt.Printf("deleted: %+v\n", del)
		if err != nil {
			panic(err)
		}*/
}
