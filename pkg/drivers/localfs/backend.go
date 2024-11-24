// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

const (
	DefaultFileMode          os.FileMode = 0o444
	DefaultDirectoryMode     os.FileMode = 0o755
	DefaultDataFileOpenFlags             = os.O_WRONLY | os.O_CREATE | os.O_SYNC |
		syscall.O_NOATIME | syscall.O_NOFOLLOW | syscall.O_EXCL
)

type watcher struct {
	events        chan []*server.Event
	prefix        string
	id            int64
	startRevision int64
	isClosed      int32
}

type Backend struct {
	DataBasePath    string
	CounterFilePath string

	watchers sync.Map

	counterLock sync.Mutex
	actionsLock sync.Mutex
}

var backend *Backend

// ensure Backend implements server.Backend.
var _ server.Backend = (&Backend{})

func New(_ context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	if !filepath.IsAbs(cfg.DataSourceName) {
		return false, nil, fmt.Errorf("database path must be absolute: %s", cfg.DataSourceName)
	}

	err := os.MkdirAll(cfg.DataSourceName, DefaultDirectoryMode)
	if err != nil {
		return false, nil, fmt.Errorf("database path error: %w", err)
	}

	backend = new(Backend)
	backend.DataBasePath = cfg.DataSourceName
	backend.CounterFilePath = filepath.Join(cfg.DataSourceName, DefaultCounterFilePath)

	return true, backend, nil
}

func init() {
	drivers.Register("localfs", New)
}
