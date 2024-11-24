// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

type Backend struct {
	DataBasePath    string
	CounterFilePath string

	sync.Mutex
}

var backend *Backend

// ensure Backend implements server.Backend.
var _ server.Backend = (&Backend{})

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	if !filepath.IsAbs(cfg.DataSourceName) {
		return false, nil, fmt.Errorf("database path must be absolute: %s", cfg.DataSourceName)
	}

	backend = new(Backend)
	backend.DataBasePath = cfg.DataSourceName
	backend.CounterFilePath = filepath.Join(cfg.DataSourceName, DefaultCounterFilePath)

	return true, backend, nil
}

func init() {
	drivers.Register("localfs", New)
}
