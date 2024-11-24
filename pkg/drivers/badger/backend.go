// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"

	badgerdb "github.com/dgraph-io/badger/v4"
	badgerdb_options "github.com/dgraph-io/badger/v4/options"
)

type watcher struct {
	events        chan []*server.Event
	prefix        string
	id            int64
	startRevision int64
	isClosed      int32
}

type Backend struct {
	db       *badgerdb.DB
	watchers sync.Map
}

var backend *Backend

// ensure Backend implements server.Backend.
var _ server.Backend = (&Backend{})

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	if !filepath.IsAbs(cfg.DataSourceName) {
		return false, nil, fmt.Errorf("database path must be absolute: %s", cfg.DataSourceName)
	}

	db, err := badgerdb.Open(
		badgerdb.DefaultOptions(cfg.DataSourceName).
			WithChecksumVerificationMode(badgerdb_options.OnTableAndBlockRead).
			WithCompression(badgerdb_options.CompressionType(badgerdb_options.ZSTD)).
			WithNumVersionsToKeep(5).
			WithSyncWrites(true),
	)
	if err != nil {
		return false, nil, fmt.Errorf("database path error: %w", err)
	}

	backend = new(Backend)
	backend.db = db

	// TODO: No method on backend.Driver exists to indicate a shutdown.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)

	go func() {
		<-ctx.Done()
		stop()

		_ = backend.db.Close()
	}()

	return true, backend, nil
}

func init() {
	drivers.Register("badger", New)
}
