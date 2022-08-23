package main

import (
	"context"

	"github.com/AlekSi/pointer"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/driver"
)

// This file contains examples how to use transactions

type TigrisDB struct {
	Driver driver.Driver
}

// InTransaction wraps the given function f in a transaction.
// If f returns an error, the transaction is rolled back.
// Errors are wrapped with lazyerrors.Error,
// so the caller needs to use errors.Is to check the error,
// for example, errors.Is(err, *Driver.Error).
func (tdb *TigrisDB) InTransaction(ctx context.Context, db string, f func(tx driver.Tx) error) (err error) {
	var tx driver.Tx
	if tx, err = tdb.Driver.BeginTx(ctx, db); err != nil {
		err = err
		return
	}

	defer func() {
		if err == nil {
			return
		}
		if rerr := tx.Rollback(ctx); rerr != nil {
			// log.Warningf("failed to perform rollback", rerr)
		}
	}()

	if err = f(tx); err != nil {
		err = err
		return
	}

	if err = tx.Commit(ctx); err != nil {
		err = err
		return
	}

	return
}

// IsNotFound returns true if the error is "not found" error.
// This function is implemented to keep nolint in a single place.
func IsNotFound(err error) bool {
	e, _ := err.(*driver.Error)
	return pointer.Get(e).Code == api.Code_NOT_FOUND //nolint:nosnakecase // Tigris named their const that way
}

// collectionExists returns true if collection exists.
func collectionExists(ctx context.Context, db driver.Database, collection string) (bool, error) {
	_, err := db.DescribeCollection(ctx, collection)
	switch err := err.(type) {
	case nil:
		return true, nil
	case *driver.Error:
		if IsNotFound(err) {
			return false, nil
		}
		return false, err
	default:
		return false, err
	}
}

// somefunc is an example of a func that uses transaction.
func somefunc(ctx context.Context, db, collection string) error {
	tdb := &TigrisDB{}
	return tdb.InTransaction(ctx, db, func(tx driver.Tx) error {
		_, err := collectionExists(ctx, tx, collection)
		return err
	})
}
