package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// Watch reuses the read-only list operation: one poll/second, only changed snapshots
// printed, no daemon subscription queue or durable writes. Re-run after an outage.
func watchCommand(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	asJSON := jsonFlag(fs)
	if err := parse(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return usageErr("watch [-json] takes no positional arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rpcDeadline = time.Now().Add(10 * time.Second)
	c, err := dial()
	if err != nil {
		return err
	}
	defer c.nc.Close()
	stopClose := context.AfterFunc(ctx, func() { c.nc.Close() })
	defer stopClose()
	if err := c.needCoordination(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var previous []byte
	for {
		if err := c.nc.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		var sessions []broker.SessionInfo
		if err := c.call(broker.Request{Op: "list"}, &sessions); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		encoded, err := json.Marshal(sessions)
		if err != nil {
			return err
		}
		if !bytes.Equal(previous, encoded) {
			previous = encoded
			if *asJSON {
				err = encodeOut(struct {
					Event    string               `json:"event"`
					At       time.Time            `json:"at"`
					Sessions []broker.SessionInfo `json:"sessions"`
				}{"snapshot", time.Now(), sessions})
			} else {
				err = writeOut(func(w io.Writer) error {
					fmt.Fprintf(w, "\n--- %s ---\n", time.Now().Format(time.RFC3339))
					printList(w, os.Stderr, sessions, true)
					return nil
				})
			}
			if err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}
