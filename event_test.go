package dbr

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

type testTraceReceiver struct {
	NullEventReceiver
	started           []struct{ eventName, query string }
	errored, finished int
}

func (t *testTraceReceiver) SpanStart(ctx context.Context, eventName, query string) context.Context {
	t.started = append(t.started, struct{ eventName, query string }{eventName, query})
	return ctx
}
func (t *testTraceReceiver) SpanError(ctx context.Context, err error) { t.errored++ }
func (t *testTraceReceiver) SpanFinish(ctx context.Context)           { t.finished++ }

type testRequireTraceReceiver struct {
	expected     []evt
	actual       []evt
	expectedErrs int
	actualErrs   int
}

var _ TracingEventReceiver = &testRequireTraceReceiver{}

type evt struct {
	eventName string
}

func newRequireTraceReceiver() *testRequireTraceReceiver {
	return &testRequireTraceReceiver{}
}

func (t *testRequireTraceReceiver) Event(eventName string) {
	t.actual = append(t.actual, evt{eventName})
}

func (t *testRequireTraceReceiver) EventKv(eventName string, kvs map[string]string) {
	t.actual = append(t.actual, evt{eventName})
}

func (t *testRequireTraceReceiver) EventErr(eventName string, err error) error {
	t.actualErrs++
	return nil
}

func (t *testRequireTraceReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	t.actualErrs++
	return nil
}

func (t *testRequireTraceReceiver) Timing(eventName string, nanoseconds int64) {
}

func (t *testRequireTraceReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
}

func (t *testRequireTraceReceiver) SetExpected(evts []evt) {
	t.expected = evts
}

func (t *testRequireTraceReceiver) SetExpectedErrs(count int) {
	t.expectedErrs = count
}

func (t *testRequireTraceReceiver) SpanStart(ctx context.Context, eventName, query string) context.Context {
	t.actual = append(t.actual, evt{eventName})
	return ctx
}

func (t *testRequireTraceReceiver) SpanError(ctx context.Context, err error) {
	t.actualErrs++
}

func (t *testRequireTraceReceiver) SpanFinish(ctx context.Context) {}

func (t *testRequireTraceReceiver) RequireEqual(s *testing.T) {
	require.Equal(s, t.expected, t.actual)
	require.Equal(s, t.expectedErrs, t.actualErrs)
}
