package tracker

import (
	"reflect"

	"github.com/the-anna-project/context"
)

// Service represents a service to collect and provision metrics about executed
// CLG trees.
type Service interface {
	// Boot initializes and starts the whole service like booting a machine. The
	// call to Boot blocks until the service is completely initialized, so you
	// might want to call it in a separate goroutine.
	Boot()
	// CLGCount simply counts the CLGs being executed. That is, CLGCount
	// increments a counter for this specific metric by one when being executed.
	CLGCount(ctx context.Context, args []reflect.Value) error
	// CLGCountPerCLGTree emits a gauge for the number of CLGs being executed per
	// CLG tree ID. This number may vary a lot. The challenge of this metric is
	// the "undefined" event of when no more CLGs are executed for a specific CLG
	// tree. Therefore we need to make sure there is no single event queued for a
	// specific CLG tree ID. Further we want to track a count of events that are
	// distributed across processes. Therefore we maintain an intermediate counter
	// until we are certain no CLG is queued anymore. When this is the case,
	// CLGCountPerCLGTree emits the actual gauge to the configured instrumentor.
	CLGCountPerCLGTree(ctx context.Context, args []reflect.Value) error
	// CLGID is a tracker function used by Track to resolve the CLG IDs of the
	// destination and sources provided by the current context. The collected
	// information are persisted in form of behaviour peers.
	CLGID(ctx context.Context, args []reflect.Value) error
	// CLGName is a tracker function used by Track to resolve the CLG names of the
	// destination and sources provided by the current context. The collected
	// information are persisted in form of behaviour peers.
	CLGName(ctx context.Context, args []reflect.Value) error
	// Shutdown ends all processes of the service like shutting down a machine.
	// The call to Shutdown blocks until the service is completely shut down, so
	// you might want to call it in a separate goroutine.
	Shutdown()
	// Track collects metrics about executed CLG trees. Therefore information
	// provided by the given context are used.
	Track(ctx context.Context, args []reflect.Value) error
}
