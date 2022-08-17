package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
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
			"type": "integer"
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
	base64ID := string(must(json.Marshal(id)))
	// base64ID := `"` + base64.StdEncoding.EncodeToString(id) + `"`
	filter := driver.Filter(fmt.Sprintf(`{"_id":%s}`, base64ID))
	doc := driver.Document(fmt.Sprintf(`{"_id":%s, "balance":%d}`, base64ID, int64(math.MinInt64)))

	log.Printf("Inserting: %s", doc)
	insertResp := must(db.Insert(ctx, "users", []driver.Document{doc}))
	log.Printf("%s %s", insertResp.Status, insertResp.Keys[0])

	log.Printf("Reading: %s", filter)
	iter := must(db.Read(ctx, "users", filter, nil))
	readIter(iter)

	res, err := conn.DescribeDatabase(ctx, "test")
	assert(err)

	log.Printf("Database %s size %d\n", res.Db, res.Size)
	for _, collection := range res.Collections {
		log.Printf("Collection %s size %d\n", collection.Collection, collection.Size)
	}
}
