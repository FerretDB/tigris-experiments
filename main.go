package main

import (
	"context"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

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

func main() {
	ctx := context.Background()

	conn := must(driver.NewDriver(ctx, &config.Driver{
		URL: "127.0.0.1:8081",
	}))

	_ = conn.DropDatabase(ctx, "test")
	assert(conn.CreateDatabase(ctx, "test"))
	db := conn.UseDatabase("test")

	collNum := runtime.GOMAXPROCS(-1) * 10

	ready := make(chan struct{}, collNum)
	start := make(chan struct{})

	var created atomic.Int32 // number of successful attempts to create a collection
	var wg sync.WaitGroup
	for i := 0; i < collNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			ready <- struct{}{}

			<-start

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
			err := db.CreateOrUpdateCollection(ctx, "users", schema)

			if err == nil {
				created.Add(1)
				log.Printf("iteration %d, success\n", i)
			} else {
				log.Printf("iteration %d, error: %v\n", i, err)
			}
		}(i)
	}

	for i := 0; i < collNum; i++ {
		<-ready
	}

	close(start)

	wg.Wait()

	log.Printf("Successfully created: %d\n\n", created.Load())
}
