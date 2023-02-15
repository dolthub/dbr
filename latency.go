package dbr

import (
	"context"
	"time"
)

type dbrLatencyMetadataKey struct{}
type dbrQueryMetadataKey struct{}

// DbrLatencyMetadata stores metadata used for determining the latency of a dbr event.
type DbrLatencyMetadata struct {
	EventName string
	StartTime time.Time
}

// WithDbrLatencyMetadata stores DbrLatencyMetadata in the provided context.
func WithDbrLatencyMetadata(ctx context.Context, dbrm DbrLatencyMetadata) context.Context {
	return context.WithValue(ctx, dbrLatencyMetadataKey{}, dbrm)
}

// GetDbrLatencyMetadata returns the DbrLatencyMetadata and a boolean indicating if it was found.
func GetDbrLatencyMetadata(ctx context.Context) (dbrm DbrLatencyMetadata, ok bool) {
	dbrm, ok = ctx.Value(dbrLatencyMetadataKey{}).(DbrLatencyMetadata)
	return
}

// DbrQueryMetadata stores metadata used for tracking a query identifier.
type DbrQueryMetadata struct {
	QueryId string
}

// WithDbrQueryMetadata stores DbrQueryMetadata in the provided context.
func WithDbrQueryMetadata(ctx context.Context, dbrm DbrQueryMetadata) context.Context {
	return context.WithValue(ctx, dbrQueryMetadataKey{}, dbrm)
}

// GetDbrQueryMetadata returns the DbrQueryMetadata and a boolean indicateing if it was found.
func GetDbrQueryMetadata(ctx context.Context) (dbrm DbrQueryMetadata, ok bool) {
	dbrm, ok = ctx.Value(dbrQueryMetadataKey{}).(DbrQueryMetadata)
	return
}

// NewContextWithMetricValues returns a new context containing the DbrLatencyMetadata
// and DbrQueryMetadata from the provided context.
func NewContextWithMetricValues(ctx context.Context) context.Context {
	newCtx := context.Background()
	dqm, ok := GetDbrQueryMetadata(ctx)
	if ok {
		newCtx = WithDbrQueryMetadata(newCtx, dqm)
	}
	dlm, ok := GetDbrLatencyMetadata(ctx)
	if ok {
		newCtx = WithDbrLatencyMetadata(newCtx, dlm)
	}
	return newCtx
}
