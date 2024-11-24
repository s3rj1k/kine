// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/k3s-io/kine/pkg/server"

	badgerdb "github.com/dgraph-io/badger/v4"
)

func (b *Backend) Get(_ context.Context, key, _ string, limit, revision int64) (int64 /*revision*/, *server.KeyValue, error) {
	var kv *server.KeyValue

	if err := b.db.View(func(txn *badgerdb.Txn) error {
		_, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badgerdb.ErrKeyNotFound) {
				return nil
			}

			return err
		}

		kv = &server.KeyValue{
			Key: key,
		}

		keyOpts := badgerdb.DefaultIteratorOptions
		keyOpts.AllVersions = true
		keyOpts.PrefetchValues = true

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

		var (
			expiresAt  uint64
			foundValue bool
		)

		for keyIt.Rewind(); keyIt.Valid(); keyIt.Next() {
			item := keyIt.Item()
			version := int64(item.Version())

			if kv.CreateRevision == 0 || version < kv.CreateRevision {
				kv.CreateRevision = version
			}

			if (revision <= 0 || version <= revision) && (kv.ModRevision == 0 || version > kv.ModRevision) {
				kv.ModRevision = version
				expiresAt = item.ExpiresAt()
				foundValue = true

				if err := item.Value(func(val []byte) error {
					kv.Value = slices.Clone(val)

					return nil
				}); err != nil {
					return err
				}
			}
		}

		if !foundValue {
			return badgerdb.ErrKeyNotFound
		}

		if expiresAt > 0 {
			now := uint64(time.Now().Unix())
			if expiresAt > now {
				kv.Lease = int64(expiresAt - now)
			}
		}

		return nil
	}); err != nil {
		currentRev := int64(b.db.MaxVersion())
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return currentRev, nil, nil
		}

		return currentRev, nil, err
	}

	if kv == nil {
		currentRev := int64(b.db.MaxVersion())

		return currentRev, nil, nil
	}

	if kv.ModRevision == 0 || kv.CreateRevision == 0 { // MUST never happen -> illegal resource version from storage: 0
		panic(fmt.Sprintf("GET key=%s, revision=%d, currentRev=%d, foundRevs=%d,%d",
			key, revision, b.db.MaxVersion(), kv.CreateRevision, kv.ModRevision))
	}

	return kv.ModRevision, kv, nil
}
