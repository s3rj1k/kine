// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	CreateEvent = iota + 1
	UpdateEvent
	DeleteEvent

	UnknownEvent
)

type Info struct {
	CreateRevision int64
	ModRevision    int64

	CreationTime int64
	ExpireTime   int64
}

func NewInfo(loc string) (info Info) {
	parts := strings.Split(filepath.Base(loc), ".")
	if len(parts) != 4 {
		return Info{}
	}

	createRevision, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Info{}
	}

	modRevision, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Info{}
	}

	createTime, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Info{}
	}

	expiresTime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return Info{}
	}

	return Info{
		CreateRevision: createRevision,
		ModRevision:    modRevision,
		CreationTime:   createTime,
		ExpireTime:     expiresTime,
	}
}

func (i Info) String() string {
	return fmt.Sprintf("%020d.%020d.%d.%d", i.CreateRevision, i.ModRevision, i.CreationTime, i.ExpireTime)
}

func (i Info) IsZero() bool {
	return i.CreateRevision == 0 || i.ModRevision == 0 || i.CreationTime == 0
}

func (i Info) HasExpired(t time.Time) bool {
	if i.ExpireTime == 0 || t.IsZero() {
		return false
	}

	return i.ExpireTime <= t.Unix()
}

func (i Info) GetLeaseTime() int64 {
	return max(i.ExpireTime-i.CreationTime, 0)
}

func (i Info) GetEventType() int {
	if i.IsZero() {
		return UnknownEvent
	}

	if i.ExpireTime != 0 && i.ExpireTime <= i.CreationTime {
		return DeleteEvent
	}

	if i.CreateRevision == i.ModRevision {
		return CreateEvent
	}

	if i.CreateRevision < i.ModRevision {
		return UpdateEvent
	}

	return UnknownEvent
}
