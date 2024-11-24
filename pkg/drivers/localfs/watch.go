// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
)

var ErrWatchFailed = errors.New("failed to watch key")

func (*Backend) Watch(ctx context.Context, key string, revision int64) server.WatchResult {
	dbDirectory := getDataBaseDirectory()

	events := make(chan []*server.Event)

	if revision == 0 {
		revision = ReadCounter()
	}

	result := server.WatchResult{
		CurrentRevision: revision,
		CompactRevision: revision,
		Events:          events,
	}

	if key == "" {
		return result
	}

	go func() {
		defer close(events)

		fileEvents, err := MonitorFiles(ctx, filepath.Join(dbDirectory, key))
		if err != nil {
			return
		}

		for {
			select {
			case <-ctx.Done():
				return

			case fileEvent, ok := <-fileEvents:
				if !ok {
					return
				}

				// skip if doesn't match key criteria
				if !strings.HasPrefix(fileEvent.Path, key) {
					continue
				}

				// process only valid blob files
				fileName := filepath.Base(fileEvent.Path)

				info, err := NewInfo(fileName)
				if err != nil {
					continue
				}

				// skip if revision is specified and current file revision is higher
				if revision > 0 && info.Rev > revision {
					continue
				}

				switch fileEvent.Type {
				case EventCloseWrite:
					content, err := os.ReadFile(fileEvent.Path)
					if err != nil {
						continue
					}

					var isCreate bool

					{
						entries, _ := os.ReadDir(filepath.Dir(fileEvent.Path))
						isCreate = len(entries) <= 1
					}

					select {
					case events <- []*server.Event{
						{
							Create: isCreate,
							KV: &server.KeyValue{
								Key:            fileEvent.Path,
								CreateRevision: info.Rev,
								ModRevision:    info.Rev,
								Value:          content,
								Lease:          info.Expires,
							},
						},
					}:
					case <-ctx.Done():
						return
					}

				case EventMoveTo:
					select {
					case events <- []*server.Event{
						{
							Delete: true,
							PrevKV: &server.KeyValue{
								Key:            fileEvent.Path,
								CreateRevision: info.Rev,
								ModRevision:    info.Rev,
								Lease:          info.Expires,
							},
						},
					}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return result
}
