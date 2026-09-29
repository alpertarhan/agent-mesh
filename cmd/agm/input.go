package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// refList is a repeatable -ref PATH flag.
type refList []string

func (r *refList) String() string     { return strings.Join(*r, ",") }
func (r *refList) Set(v string) error { *r = append(*r, v); return nil }

// refFlag adds the repeatable -ref flag of the message verbs.
func refFlag(fs *flag.FlagSet) *refList {
	refs := &refList{}
	fs.Var(refs, "ref", "reference a file by path (repeatable); the receiver reads it, content is not sent")
	return refs
}

// trailingFlag rejects a word after <to>/<msg-id> that matches one of the command's
// own flags: it is almost always a flag put in the wrong place (e.g. a trailing
// -no-wait that would otherwise be sent as text). Unknown dash words (-v, -, --,
// -x=1) stay text, as do flag values. `--` before the target escapes back to text.
// It runs before connecting, so a mistake queues nothing.
func trailingFlag(cmd string, fs *flag.FlagSet, raw []string) error {
	// flag.Parse consumes `--` immediately before the remaining arguments.
	if n := len(raw) - fs.NArg(); n > 0 && raw[n-1] == "--" {
		return nil
	}
	for _, w := range fs.Args()[1:] {
		name := w
		switch {
		case strings.HasPrefix(name, "--"):
			name = name[2:]
		case strings.HasPrefix(name, "-") && len(name) > 1:
			name = name[1:]
		default:
			continue
		}
		if i := strings.IndexByte(name, '='); i >= 0 {
			name = name[:i]
		}
		if name == "" || fs.Lookup(name) == nil {
			continue
		}
		what := "<to>"
		if cmd == "reply" {
			what = "<msg-id>"
		}
		return usageErr("%q after %s is a flag: put flags before %s (agm %s %s %s ...), or put -- before %s to send it as text", w, what, what, cmd, w, what, what)
	}
	return nil
}

// messageBody builds text and attachments from positional words (send/ask/reply),
// or file (the *-file verbs: a path, or "-" for stdin), plus -ref paths. A positional
// "-" is literal text; only the *-file verbs read stdin.
func messageBody(words []string, file string, refs []string, stdin io.Reader) (string, []broker.Attachment, error) {
	text := strings.Join(words, " ")
	if file != "" {
		if len(words) > 0 {
			return "", nil, errors.New("give the text either as arguments or from a file, not both")
		}
		var data []byte
		var err error
		if file == "-" {
			data, err = io.ReadAll(io.LimitReader(stdin, broker.MaxFrame+1))
		} else {
			data, err = readLimited(file)
		}
		if err != nil {
			return "", nil, fmt.Errorf("read %s: %w", file, err)
		}
		if len(data) > broker.MaxMessage {
			return "", nil, fmt.Errorf("%s: %d bytes exceeds the %d byte message limit; share it with -ref instead", file, len(data), broker.MaxMessage)
		}
		if !utf8.Valid(data) {
			return "", nil, fmt.Errorf("%s: not valid UTF-8 text", file)
		}
		text = string(data) // exactly as given
		if strings.TrimSpace(text) == "" {
			return "", nil, fmt.Errorf("%s: empty message", file)
		}
	}
	var atts []broker.Attachment
	var names []string
	for _, p := range refs {
		abs, err := filepath.Abs(p) // relative to the sender's cwd
		if err != nil {
			return "", nil, fmt.Errorf("-ref %s: %w", p, err)
		}
		fi, err := os.Stat(abs)
		if err != nil {
			return "", nil, fmt.Errorf("-ref %s: %w", p, errors.Unwrap(err))
		}
		if !fi.Mode().IsRegular() {
			return "", nil, fmt.Errorf("-ref %s: not a regular file", p)
		}
		atts = append(atts, broker.Attachment{Type: "ref", Name: filepath.Base(abs), Path: abs})
		names = append(names, filepath.Base(abs))
	}
	if len(atts) > broker.MaxRefs {
		return "", nil, fmt.Errorf("at most %d -ref files", broker.MaxRefs)
	}
	if strings.TrimSpace(text) == "" {
		if len(atts) == 0 {
			return "", nil, errors.New("message text required (arguments, or send-file/ask-file/reply-file with a path or - for stdin)")
		}
		text = "Shared file(s): " + strings.Join(names, ", ")
	}
	return text, atts, nil
}

func readLimited(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, broker.MaxFrame+1))
}

// shellQuote single-quotes s for a POSIX shell.
func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/._-+@") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
