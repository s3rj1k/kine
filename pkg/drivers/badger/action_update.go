// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/k3s-io/kine/pkg/server"

	badgerdb "github.com/dgraph-io/badger/v4"
)

func (b *Backend) Update(_ context.Context, key string, value []byte, revision, lease int64) (
	currentRevision int64, newKV *server.KeyValue, updated bool, err error,
) {
	var prevKV *server.KeyValue

	err = b.db.Update(func(txn *badgerdb.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badgerdb.ErrKeyNotFound) {
				return server.ErrNotSupported
			}

			return err
		}

		if revision > 0 && int64(item.Version()) != revision {
			return server.ErrFutureRev
		}

		err = item.Value(func(val []byte) error {
			prevKV = &server.KeyValue{
				Key:         key,
				ModRevision: int64(item.Version()),
				Value:       slices.Clone(val),
			}
			return nil
		})
		if err != nil {
			return err
		}

		keyOpts := badgerdb.DefaultIteratorOptions
		keyOpts.AllVersions = true
		keyOpts.PrefetchValues = false

		// type IteratorOptions struct {
		// 	// PrefetchSize is the number of KV pairs to prefetch while iterating.
		// 	// Valid only if PrefetchValues is true.
		// 	PrefetchSize int
		// 	// PrefetchValues Indicates whether we should prefetch values during
		// 	// iteration and store them.
		// 	PrefetchValues bool
		// 	Reverse        bool // Direction of iteration. False is forward, true is backward.
		// 	AllVersions    bool // Fetch all valid versions of the same key.
		// 	InternalAccess bool // Used to allow internal access to badger keys.

		// 	Prefix  []byte // Only iterate over this given prefix.
		// 	SinceTs uint64 // Only read data that has version > SinceTs.
		// 	// contains filtered or unexported fields
		// }

		keyIt := txn.NewKeyIterator([]byte(key), keyOpts)
		defer keyIt.Close()

		prevKV.CreateRevision = prevKV.ModRevision
		for keyIt.Rewind(); keyIt.Valid(); keyIt.Next() {
			versionItem := keyIt.Item()
			prevKV.CreateRevision = min(prevKV.CreateRevision, int64(versionItem.Version()))
		}

		if expiresAt := item.ExpiresAt(); expiresAt > 0 {
			currentTime := uint64(time.Now().Unix())
			if expiresAt > currentTime {
				prevKV.Lease = int64(expiresAt - currentTime)
			}
		}

		entry := badgerdb.NewEntry([]byte(key), value)

		if lease > 0 {
			entry = entry.WithTTL(time.Duration(lease) * time.Second)
		}

		if err := txn.SetEntry(entry); err != nil {
			return err
		}

		updated = true

		return nil
	})
	if err != nil {
		return int64(b.db.MaxVersion()), nil, false, err
	}

	currentRevision = int64(b.db.MaxVersion())

	newKV = &server.KeyValue{
		Key:            key,
		CreateRevision: prevKV.CreateRevision,
		ModRevision:    currentRevision,
		Value:          value,
		Lease:          lease,
	}

	if updated {
		b.sendEvent(key, &server.Event{
			KV:     newKV,
			PrevKV: prevKV,
		})
	}

	return currentRevision, newKV, updated, nil
}
