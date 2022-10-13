package main

import (
	"context"
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
			"type": "integer",
			"format": "int64"
		},
		"_id": {
			"type": "string"
		}
	},
	"primary_key": ["_id"]
}
`))
	assert(db.CreateOrUpdateCollection(ctx, "users", schema))

	id := "foo"
	filter := driver.Filter(fmt.Sprintf(`{"_id":"%s"}`, id))
	doc := driver.Document(fmt.Sprintf(`{"_id":"%s", "balance":%d}`, id, 5))

	log.Printf("Inserting: %s", doc)
	insertResp := must(db.Insert(ctx, "users", []driver.Document{doc}))
	log.Printf("%s %s", insertResp.Status, insertResp.Keys[0])

	log.Printf("Reading: %s", filter)
	iter := must(db.Read(ctx, "users", filter, nil))
	readIter(iter)

	doc = driver.Document(fmt.Sprintf(`{"_id":"%s", "balance":"%s"}`, id, "54.43"))
	replaceResp := must(db.Replace(ctx, "users", []driver.Document{doc}))
	log.Printf("%s", replaceResp.Status)

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
