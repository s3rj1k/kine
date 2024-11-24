// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"cmp"
	"context"
	"errors"
	"time"

	"github.com/k3s-io/kine/pkg/server"

	badgerdb "github.com/dgraph-io/badger/v4"
)

func (b *Backend) Create(_ context.Context, key string, value []byte, lease int64) (revision int64, err error) {
	err = b.db.Update(func(txn *badgerdb.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			return server.ErrKeyExists
		} else if !errors.Is(err, badgerdb.ErrKeyNotFound) {
			return err
		}

		entry := badgerdb.NewEntry([]byte(key), value)

		if lease > 0 {
			entry = entry.WithTTL(time.Duration(lease) * time.Second)
		}

		if err := txn.SetEntry(entry); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return cmp.Or(revision, int64(b.db.MaxVersion())), err
	}

	err = b.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		revision = int64(item.Version())

		return nil
	})
	if err != nil {
		return cmp.Or(revision, int64(b.db.MaxVersion())), err
	}

	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: revision,
		ModRevision:    revision,
		Value:          value,
		Lease:          lease,
	}

	event := &server.Event{
		Create: true,
		KV:     kv,
		PrevKV: nil,
	}

	b.sendEvent(key, event)

	return revision, nil
}
