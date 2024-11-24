// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const (
	bufferSize = 1024 * (unix.SizeofInotifyEvent + 16)
)

type EventType int

const (
	EventCloseWrite EventType = iota
	EventMoveTo
)

type FileEvent struct {
	Type EventType
	Path string
}

type inotifyData struct {
	fd   int
	wd   uint32
	path string
}

func initializeInotify(path string) (*inotifyData, error) {
	fd, err := unix.InotifyInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing inotify: %v", err)
	}

	wd, err := unix.InotifyAddWatch(fd, path, unix.IN_CLOSE_WRITE|unix.IN_MOVED_TO)
	if err != nil {
		unix.Close(fd)

		return nil, fmt.Errorf("error adding watch: %v", err)
	}

	return &inotifyData{fd: fd, wd: uint32(wd), path: path}, nil
}

func parseEvent(buffer []byte, offset int) (*unix.InotifyEvent, string, int, error) {
	event := new(unix.InotifyEvent)

	err := binary.Read(bytes.NewReader(buffer[offset:]), binary.LittleEndian, event)
	if err != nil {
		return nil, "", 0, err
	}

	name := ""
	length := event.Len

	if length > 0 {
		if offset+unix.SizeofInotifyEvent+int(length) > len(buffer) {
			return nil, "", 0, fmt.Errorf("buffer overflow")
		}

		name = string(bytes.TrimRight(buffer[offset+unix.SizeofInotifyEvent:offset+unix.SizeofInotifyEvent+int(length)], "\x00"))
	}

	newOffset := offset + unix.SizeofInotifyEvent + int(length)

	return event, name, newOffset, nil
}

func processEvents(ctx context.Context, buffer []byte, path string, eventChan chan<- FileEvent) {
	offset := 0

	for offset < len(buffer) {
		if len(buffer)-offset < unix.SizeofInotifyEvent {
			break
		}

		event, name, newOffset, err := parseEvent(buffer, offset)
		if err != nil {
			break
		}

		offset = newOffset

		var fileEvent FileEvent

		switch {
		case event.Mask&unix.IN_CLOSE_WRITE != 0:
			fileEvent = FileEvent{
				Type: EventCloseWrite,
				Path: filepath.Join(path, name),
			}
		case event.Mask&unix.IN_MOVED_TO != 0:
			fileEvent = FileEvent{
				Type: EventMoveTo,
				Path: filepath.Join(path, name),
			}
		default:
			continue
		}

		select {
		case eventChan <- fileEvent:
		case <-ctx.Done():
			return
		}
	}
}

func monitorFileEvents(ctx context.Context, data *inotifyData, eventChan chan<- FileEvent) {
	defer func() {
		_, _ = unix.InotifyRmWatch(data.fd, data.wd)
		_ = unix.Close(data.fd)

		close(eventChan)
	}()

	buffer := make([]byte, bufferSize)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := unix.Read(data.fd, buffer)
			if err != nil {
				if errors.Is(err, unix.EINTR) {
					continue
				}

				return
			}

			processEvents(ctx, buffer[:n], data.path, eventChan)
		}
	}
}

func MonitorFiles(ctx context.Context, path string) (<-chan FileEvent, error) {
	eventChan := make(chan FileEvent)

	data, err := initializeInotify(path)
	if err != nil {
		return nil, err
	}

	go monitorFileEvents(ctx, data, eventChan)

	return eventChan, nil
}
