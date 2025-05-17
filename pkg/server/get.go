package server

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if r.Limit != 0 && len(r.RangeEnd) != 0 {
		return nil, fmt.Errorf("invalid combination of rangeEnd and limit, limit should be 0 got %d", r.Limit)
	}

	rev, kv, err := l.backend.Get(ctx, string(r.Key), string(r.RangeEnd), r.Limit, r.Revision)
	logrus.Tracef("GET key=%s, revision=%d", r.Key, rev)
	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
		resp.Count = 1
	}
	return resp, err
}
