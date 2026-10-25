package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func TestBridgeWriteDeadlineAndCancellation(t *testing.T) {
	for _, cancelContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "context"}[cancelContext], func(t *testing.T) {
			local, remote := net.Pipe()
			defer remote.Close()
			c := &bconn{nc: local, waiters: map[int64]*bwaiter{}, dead: make(chan struct{})}
			defer c.close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			budget := 30 * time.Millisecond
			if cancelContext {
				budget = time.Minute
				stop := time.AfterFunc(30*time.Millisecond, cancel)
				defer stop.Stop()
			}
			done := make(chan error, 2)
			// The second call may wait on the write mutex; cancellation must free both.
			for range 2 {
				go func() {
					done <- c.callDL(ctx, broker.Request{Op: "protocol"}, nil, fixedDL(budget))
				}()
			}
			for range 2 {
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("non-reading peer unexpectedly answered")
					}
				case <-time.After(time.Second):
					t.Fatal("blocked writer ignored deadline/cancellation")
				}
			}
		})
	}
}

func TestBridgeRetriesStorageFailure(t *testing.T) {
	if !bridgeTransient(broker.CodePersistence) {
		t.Fatal("temporary storage failure would bounce a queued message")
	}
}
