// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/k3s-io/kine/pkg/server"

	badgerdb "github.com/dgraph-io/badger/v4"
)

func (b *Backend) List(_ context.Context, prefix, startKey string, limit, revision int64) (
	maxRevision int64, kvs []*server.KeyValue, err error,
) {
	err = b.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = true
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

		seenKeys := make(map[string]*server.KeyValue)

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

			maxRevision = max(maxRevision, version)

			if kv, exists := seenKeys[key]; exists {
				if kv.ModRevision >= version {
					continue
				}
			}

			var value []byte

			err := item.Value(func(val []byte) error {
				value = slices.Clone(val)

				return nil
			})
			if err != nil {
				return err
			}

			kv := &server.KeyValue{
				Key:            key,
				CreateRevision: version,
				ModRevision:    version,
				Value:          value,
			}

			keyOpts := badgerdb.DefaultIteratorOptions
			keyOpts.AllVersions = true
			keyOpts.PrefetchValues = false

			keyIt := txn.NewKeyIterator([]byte(key), keyOpts)
			for keyIt.Rewind(); keyIt.Valid(); keyIt.Next() {
				versionItem := keyIt.Item()

				kv.CreateRevision = min(kv.CreateRevision, int64(versionItem.Version()))
			}
			keyIt.Close()

			if expiresAt := item.ExpiresAt(); expiresAt > 0 {
				currentTime := uint64(time.Now().Unix())
				if expiresAt > currentTime {
					kv.Lease = int64(expiresAt - currentTime)
				}
			}

			seenKeys[key] = kv
		}

		for _, kv := range seenKeys {
			kvs = append(kvs, kv)
		}

		slices.SortFunc(kvs, func(a, b *server.KeyValue) int {
			return strings.Compare(a.Key, b.Key)
		})

		if limit > 0 && len(kvs) > int(limit) {
			kvs = slices.Clip(kvs[:limit])
		}

		return nil
	})
	if err != nil {
		return cmp.Or(maxRevision, int64(b.db.MaxVersion())), nil, err
	}

	return cmp.Or(maxRevision, int64(b.db.MaxVersion())), kvs, nil
}
