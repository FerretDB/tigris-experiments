package main

import (
	"context"
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

	_, _ = conn.DeleteProject(ctx, "test")
	_, err := conn.CreateProject(ctx, "test")
	assert(err)
	db := conn.UseDatabase("test")

	schema := driver.Schema(strings.TrimSpace(`
	{
		"title": "users",
		"primary_key": ["_id"],
		"properties": {
			"v": {"type": "integer", "format": "int64"},
			"_id": {"type": "string"}
		}
	}
`))
	assert(db.CreateOrUpdateCollection(ctx, "users", schema))

	filter := driver.Filter(`{"_id":"int64"}`)
	doc := driver.Document(`{"_id":"int64","v":42}`)

	log.Printf("Inserting: %s", doc)
	insertResp := must(db.Insert(ctx, "users", []driver.Document{doc}))
	log.Printf("%s %s", insertResp.Status, insertResp.Keys[0])

	log.Printf("Reading: %s", filter)
	iter := must(db.Read(ctx, "users", filter, nil))
	readIter(iter)

	replace := driver.Document(`{"_id":"int64","v":[]}`)
	updateResp := must(db.Replace(ctx, "users", []driver.Document{replace})) // Internal server error
	log.Printf("%s %d", updateResp.Status, updateResp.Metadata)

	log.Printf("Reading after update: %s", filter)
	iter = must(db.Read(ctx, "users", filter, nil))
	readIter(iter)
}
