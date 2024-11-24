// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"cmp"
	"context"
	"strings"

	badgerdb "github.com/dgraph-io/badger/v4"
)

func (b *Backend) Count(_ context.Context, prefix, startKey string, revision int64) (
	maxRevision int64, count int64, err error,
) {
	err = b.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = true

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

		it := txn.NewIterator(opts)
		defer it.Close()

		prefixBytes := []byte(prefix)
		startKeyBytes := []byte(startKey)

		if len(startKey) > 0 && strings.HasPrefix(startKey, prefix) {
			it.Seek(startKeyBytes)
		} else {
			it.Seek(prefixBytes)
		}

		countedKeys := make(map[string]struct{})

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			version := int64(item.Version())

			if !strings.HasPrefix(key, prefix) {
				break
			}

			if len(startKey) > 0 && key < startKey {
				continue
			}

			if revision > 0 && version > revision {
				continue
			}

			if _, counted := countedKeys[key]; !counted {
				count++
				countedKeys[key] = struct{}{}
			}

			maxRevision = max(maxRevision, version)
		}

		return nil
	})

	return cmp.Or(maxRevision, revision, int64(b.db.MaxVersion())), count, err
}
