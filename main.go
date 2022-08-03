package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
)

func assert(err error) {
	if err != nil {
		panic(err)
	}
}

func must[T any](v T, err error) T {
	assert(err)
	return v
}

func readIter(iter driver.Iterator) {
	var d driver.Document
	for iter.Next(&d) {
		log.Printf("\t%s", d)
	}
	assert(iter.Err())
	iter.Close()
	log.Printf("\tDONE")
}

func main() {
	ctx := context.Background()

	conn := must(driver.NewDriver(ctx, &config.Driver{
		URL: "127.0.0.1:8081",
	}))

	_ = conn.DropDatabase(ctx, "test")
	assert(conn.CreateDatabase(ctx, "test"))
	db := conn.UseDatabase("test")

	schema := driver.Schema(strings.TrimSpace(`
{
	"title": "users",
	"properties": {
		"balance": {
			"type": "number"
		},
		"_id": {
			"type": "string",
			"format": "byte"
		}
	},
	"primary_key": ["_id"]
}
`))
	assert(db.CreateOrUpdateCollection(ctx, "users", schema))

	id := must(hex.DecodeString("62ea6a943d44b10e1b6b8797"))
	base64ID := base64.StdEncoding.EncodeToString(id)
	filter := driver.Filter(fmt.Sprintf(`{"_id":%q}`, base64ID))
	doc := driver.Document(fmt.Sprintf(`{"_id":%q, "balance":1}`, base64ID))

	log.Printf("Inserting: %s", doc)
	insertResp := must(db.Insert(ctx, "users", []driver.Document{doc}))
	log.Printf("%s %s", insertResp.Status, insertResp.Keys[0])

	log.Printf("Reading: %s", filter)
	iter := must(db.Read(ctx, "users", filter, nil))
	readIter(iter)

	updateResp := must(db.Update(ctx, "users", filter, driver.Update(`{"$set": {"balance": 2}}`)))
	log.Printf("%s %d", updateResp.Status, updateResp.ModifiedCount)

	log.Printf("Reading after update: %s", filter)
	iter = must(db.Read(ctx, "users", filter, nil))
	readIter(iter)

	log.Printf("Deleting: %s", filter)
	deleteResp, err := db.Delete(ctx, "users", filter)
	log.Printf("%v %s", deleteResp, err)

	log.Printf("Reading after delete: %s", filter)
	iter = must(db.Read(ctx, "users", filter, nil))
	readIter(iter)
}
