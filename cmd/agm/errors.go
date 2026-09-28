package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// cliError is a CLI-side failure with a stable code for -json error output. Daemon
// errors keep their own codes (*broker.Error).
type cliError struct {
	Code string
	Err  error
}

func (e *cliError) Error() string { return e.Err.Error() }
func (e *cliError) Unwrap() error { return e.Err }

// CLI error codes (see docs/cli.md).
const (
	codeUsage       = "usage"              // bad flags or arguments (exit 2)
	codeNoIdentity  = "no_identity"        // no session id could be inferred
	codeUnavailable = "daemon_unavailable" // daemon could not be reached or started
	codeTransport   = "transport"          // connection failed mid-request: outcome unknown
	codeTimeout     = "timeout"            // no reply before the deadline
	codeInput       = "invalid_input"      // message text/file/ref rejected before sending
	codeTooLarge    = "too_large"          // request over the protocol frame
	codeOutput      = "output"             // writing the result failed (nothing acked)
	codeOutdated    = "daemon_outdated"    // the running daemon predates a feature: restart it
)

func coded(code string, err error) error {
	if err == nil {
		return nil
	}
	var ce *cliError
	var be *broker.Error
	if errors.As(err, &ce) || errors.As(err, &be) {
		return err // keep the most specific code
	}
	return &cliError{Code: code, Err: err}
}

func usageErr(format string, a ...any) error {
	return &cliError{Code: codeUsage, Err: fmt.Errorf(format, a...)}
}

// jsonOut is the -json flag of the running command (errors then go to stderr as JSON).
var jsonOut *bool

func jsonFlag(fs *flag.FlagSet) *bool {
	jsonOut = fs.Bool("json", false, "json output (stdout); errors as {\"error\":{...}} on stderr")
	return jsonOut
}

// parse parses subcommand flags without exiting or printing usage chatter.
func parse(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(os.Stderr)
			fmt.Fprintf(os.Stderr, "usage of %s:\n", fs.Name())
			fs.PrintDefaults()
			return exitCode(0)
		}
		return usageErr("%s: %v", fs.Name(), err)
	}
	return nil
}

// writeOut writes a command's stdout through a buffer and reports any write error,
// so callers act (e.g. ack) only after the output really went out.
func writeOut(f func(w io.Writer) error) error {
	bw := bufio.NewWriter(os.Stdout)
	if err := f(bw); err != nil {
		return coded(codeOutput, err)
	}
	return coded(codeOutput, bw.Flush())
}

func encodeOut(v any) error {
	return writeOut(func(w io.Writer) error { return json.NewEncoder(w).Encode(v) })
}

func printlnOut(s string) error {
	return writeOut(func(w io.Writer) error { _, err := fmt.Fprintln(w, s); return err })
}

// reportError prints err for main: JSON on stderr in -json mode, else "agm: ...".
// It returns the exit status.
func reportError(err error, w io.Writer) int {
	code, msg := "error", err.Error()
	var ce *cliError
	var be *broker.Error
	switch {
	case errors.As(err, &be):
		code, msg = be.Code, be.Message
	case errors.As(err, &ce):
		code = ce.Code
	}
	if jsonOut != nil && *jsonOut {
		json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
	} else {
		fmt.Fprintln(w, "agm:", err)
	}
	if code == codeUsage {
		return 2
	}
	return 1
}
