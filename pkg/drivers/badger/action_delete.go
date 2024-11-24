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

func (b *Backend) Delete(_ context.Context, key string, _ int64 /*revision*/) (
	currentRevision int64, prevKV *server.KeyValue, deleted bool, err error,
) {
	err = b.db.Update(func(txn *badgerdb.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badgerdb.ErrKeyNotFound) {
				return nil
			}

			return err
		}

		prevKV = &server.KeyValue{
			Key: key,
		}

		err = item.Value(func(val []byte) error {
			prevKV.Value = slices.Clone(val)
			prevKV.ModRevision = int64(item.Version())
			prevKV.CreateRevision = int64(item.Version())

			return nil
		})
		if err != nil {
			return err
		}

		if expiresAt := item.ExpiresAt(); expiresAt > 0 {
			currentTime := uint64(time.Now().Unix())
			if expiresAt > currentTime {
				prevKV.Lease = int64(expiresAt - currentTime)
			}
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

		for keyIt.Rewind(); keyIt.Valid(); keyIt.Next() {
			versionItem := keyIt.Item()
			prevKV.CreateRevision = min(prevKV.CreateRevision, int64(versionItem.Version()))
		}

		if err := txn.Delete([]byte(key)); err != nil {
			return err
		}

		deleted = true

		return nil
	})
	if err != nil {
		return int64(b.db.MaxVersion()), nil, false, err
	}

	currentRevision = int64(b.db.MaxVersion())

	if deleted && prevKV != nil {
		b.sendEvent(key, &server.Event{
			Delete: true,
			KV: &server.KeyValue{
				Key:         key,
				ModRevision: currentRevision,
			},
			PrevKV: prevKV,
		})
	}

	return currentRevision, prevKV, deleted, nil
}
