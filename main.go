package main

import (
	"context"
	"fmt"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"log"
	"strings"
	"sync"
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

	collNum := 10

	ready := make(chan struct{}, collNum)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < collNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			ready <- struct{}{}

			<-start

			dbname := "test"
			conn.DescribeDatabase(ctx, dbname)
			err := conn.CreateDatabase(ctx, dbname)
			if err != nil {
				if strings.Contains(err.Error(), "duplicate key value, violates key constraint") {
					// Code = {api.Code} Code_UNKNOWN (2)
					// Message = {string} "duplicate key value, violates key constraint"
					panic(err)
				}
			}

			collName := fmt.Sprintf("stress_%d", i)

			conn.UseDatabase(dbname).DescribeCollection(ctx, collName)

			schema := driver.Schema(strings.TrimSpace(fmt.Sprintf(`{
				"title": "%s",
				"description": "Create Collection Stress %d",
				"primary_key": ["_id"],
				"properties": {
					"_id": {"type": "string"},
					"v": {"type": "string"}
				}
			}`, collName, i,
			)))

			conn.UseDatabase(dbname).CreateOrUpdateCollection(ctx, collName, schema)
		}(i)
	}

	for i := 0; i < collNum; i++ {
		<-ready
	}

	close(start)

	wg.Wait()

}
