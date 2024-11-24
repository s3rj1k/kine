// SPDX-License-Identifier: Apache-2.0

package localfs_test

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/localfs"
	"github.com/k3s-io/kine/pkg/server"
)

func requireKeysSorted(t *testing.T, kvs []*server.KeyValue) {
	t.Helper()

	sorted := slices.IsSortedFunc(kvs, func(a, b *server.KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})

	if !sorted {
		t.Fatalf("keys are not sorted")
	}
}

func requireKeys(t *testing.T, kvs []*server.KeyValue, keys ...string) bool {
	t.Helper()

	kvsKeys := make([]string, 0, len(kvs))

	for _, kv := range kvs {
		kvsKeys = append(kvsKeys, kv.Key)
	}

	equal := slices.Compare(kvsKeys, keys) == 0
	if !equal {
		t.Fatalf("Keys not in expected order:\nGot: %v\nExpected: %v", kvsKeys, keys)
	}

	return equal
}

func setupBackend(t *testing.T) server.Backend {
	tmpDir, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatalf("DB path error: %v", err)
	}

	ok, backend, err := localfs.New(context.Background(), &drivers.Config{
		Scheme:         "localfs",
		DataSourceName: tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to setup backend: %v", err)
	}

	if !ok {
		t.Fatal("Failed to setup backend")
	}

	return backend
}
