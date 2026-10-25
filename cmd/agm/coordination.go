package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func (c *client) needCoordination() error {
	if err := c.needProtocol("coordination"); err != nil {
		return err
	}
	if c.coordination < broker.CoordinationSupport {
		exe, _ := os.Executable()
		return coded(codeOutdated, fmt.Errorf("the running daemon lacks coordination support; restart it with this binary: %s restart", shellQuote(cmp(exe, "agm"))))
	}
	return nil
}

func (c *client) needTaskLifecycle() error {
	if err := c.needCoordination(); err != nil {
		return err
	}
	if c.taskLifecycle < broker.TaskLifecycleSupport {
		exe, _ := os.Executable()
		return coded(codeOutdated, fmt.Errorf("the running daemon lacks linked task states; restart it with this binary: %s restart", shellQuote(cmp(exe, "agm"))))
	}
	return nil
}

func taskCommand(as string, args []string) error {
	if len(args) == 0 || (args[0] != "set" && args[0] != "clear") {
		return usageErr("task set [-json] [-request MSG] [-state STATE] [-scope PATH]... [-waiting-for TEXT] <summary...> | task clear [-json]")
	}
	fs := flag.NewFlagSet("task "+args[0], flag.ContinueOnError)
	asJSON := jsonFlag(fs)
	var scope refList
	var waiting, state, requestID string
	if args[0] == "set" {
		fs.Var(&scope, "scope", "declared working path (repeatable; does not read or lock files)")
		fs.StringVar(&waiting, "waiting-for", "", "blocker or peer/reply being awaited")
		fs.StringVar(&state, "state", "", "explicit task state: accepted, working, blocked, or done")
		fs.StringVar(&requestID, "request", "", "original received message id (retained on subsequent updates)")
	}
	if err := parse(fs, args[1:]); err != nil {
		return err
	}
	if state != "" && !broker.ValidTaskState(state) {
		return usageErr("task -state must be accepted, working, blocked, or done")
	}
	task := &broker.TaskInfo{State: state, RequestID: requestID}
	if args[0] == "clear" {
		if fs.NArg() != 0 {
			return usageErr("task clear [-json] takes no positional arguments")
		}
	} else {
		task.Summary, task.WaitingFor = strings.Join(fs.Args(), " "), waiting
		if strings.TrimSpace(task.Summary) == "" {
			return usageErr("task set needs a summary")
		}
		if err := trailingFlag("task set", fs, args[1:]); err != nil {
			return err
		}
		for _, p := range scope {
			abs, err := filepath.Abs(p)
			if err != nil {
				return coded(codeInput, fmt.Errorf("task scope: %w", err))
			}
			task.Scope = append(task.Scope, abs)
		}
	}
	rpcDeadline = time.Now().Add(10 * time.Second)
	c, err := session(as, broker.SessionInfo{})
	if err != nil {
		return err
	}
	defer c.nc.Close()
	if task.State != "" || task.RequestID != "" {
		if err := c.needTaskLifecycle(); err != nil {
			return err
		}
	} else if err := c.needCoordination(); err != nil {
		return err
	}
	if err := c.call(broker.Request{Op: "task", Task: task}, nil); err != nil {
		return err
	}
	if *asJSON {
		return encodeOut(map[string]any{"session": c.id, "updated": true, "cleared": args[0] == "clear"})
	}
	return printlnOut("task " + map[string]string{"set": "updated", "clear": "cleared"}[args[0]])
}

func metricColumns(p *broker.Presence) (tps, ttft, context, age string) {
	tps, ttft, context, age = "unknown", "unknown", "unknown", "-"
	if p == nil {
		return
	}
	if c := p.Context; c != nil && !p.Stale && c.Window > 0 {
		context = fmt.Sprintf("%.1f%%", 100*float64(c.Tokens)/float64(c.Window))
	}
	if m := p.Metrics; m != nil {
		approx := ""
		if m.TTFTSource == "observed" {
			approx = "~"
		}
		if m.TPS != nil {
			tps = fmt.Sprintf("%s%.1f", approx, *m.TPS)
		}
		if m.TTFTMS != nil {
			ttft = fmt.Sprintf("%s%.0fms", approx, *m.TTFTMS)
		}
		if elapsed := time.Since(m.ObservedAt); elapsed < 0 {
			age = "clock-skew"
		} else {
			age = elapsed.Round(time.Second).String()
		}
	}
	if p.Stale {
		age += " (stale)"
	}
	return
}

func printCoordination(w io.Writer, s broker.SessionInfo) {
	fmt.Fprintf(w, "activity %s\n", peer(cmp(s.Activity, "unknown")))
	if p := s.Presence; p != nil {
		stale := ""
		if p.Stale {
			stale = " (stale)"
		}
		fmt.Fprintf(w, "model    %s / %s%s\n", peer(cmp(p.Provider, "unknown")), peer(cmp(p.Model, "unknown")), stale)
		fmt.Fprintf(w, "observed %s\nsince    %s\nprogress %s\n", p.UpdatedAt.Format(time.RFC3339), p.StateSince.Format(time.RFC3339), p.ProgressAt.Format(time.RFC3339))
		if len(p.ActiveTools) > 0 {
			fmt.Fprintf(w, "tools    %s%s\n", peer(strings.Join(p.ActiveTools, ", ")), stale)
		}
		tps, ttft, context, age := metricColumns(p)
		fmt.Fprintf(w, "context  %s\nlast TPS %s tokens/s\nlast TTFT %s\nmetric age %s\n", context, tps, ttft, age)
		if c := p.Context; c != nil {
			fmt.Fprintf(w, "tokens   %d / %d (context estimate)%s\n", c.Tokens, c.Window, stale)
		}
		if m := p.Metrics; m != nil {
			fmt.Fprintf(w, "response %s / %s (%s model)\ntiming   %s TTFT; %.0fms request; %d output tokens\n", peer(cmp(m.Provider, "unknown")), peer(cmp(m.Model, "unknown")), peer(cmp(m.ModelSource, "unknown")), peer(m.TTFTSource), m.DurationMS, m.OutputTokens)
		}
	} else {
		fmt.Fprintln(w, "presence unknown (adapter has not reported)")
	}
	if task := s.Task; task != nil {
		fmt.Fprintf(w, "task     %s\nupdated  %s\n", peer(task.Summary), task.UpdatedAt.Format(time.RFC3339))
		if task.State != "" {
			fmt.Fprintf(w, "task state %s (owner-declared)\nrequest  %s\n", peer(task.State), peer(task.RequestID))
		}
		for _, p := range task.Scope {
			fmt.Fprintf(w, "scope    %s\n", peer(p))
		}
		if task.WaitingFor != "" {
			fmt.Fprintf(w, "waiting  %s\n", peer(task.WaitingFor))
		}
	}
}
