// Package raftlock holds repo-level checks. The code lives under internal/ and cmd/.
package raftlock

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// commands returns the make and go command lines inside README code blocks, with any "$ " prompt removed.
func commands(readme string) []string {
	var out []string
	inBlock := false
	for _, line := range strings.Split(readme, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "```") {
			inBlock = !inBlock
			continue
		}
		line = strings.TrimPrefix(line, "$ ")
		if inBlock && (strings.HasPrefix(line, "make ") || line == "make" || strings.HasPrefix(line, "go ")) {
			out = append(out, line)
		}
	}
	return out
}

var targetRE = regexp.MustCompile(`(?m)^([A-Za-z0-9_.-]+):`)

// missing lists every make target and ./path a command refers to that is not in this repo.
func missing(cmds []string, makefile string, dirExists func(string) bool) []string {
	var targets []string
	for _, m := range targetRE.FindAllStringSubmatch(makefile, -1) {
		targets = append(targets, m[1])
	}
	var out []string
	for _, c := range cmds {
		f := strings.Fields(c)
		switch f[0] {
		case "make":
			for _, t := range f[1:] {
				if !strings.HasPrefix(t, "-") && !strings.Contains(t, "=") && !slices.Contains(targets, t) {
					out = append(out, c+": no make target "+t)
				}
			}
		case "go":
			for _, a := range f[1:] {
				if !strings.HasPrefix(a, "./") {
					continue
				}
				dir := strings.TrimSuffix(strings.TrimSuffix(a, "..."), "/")
				if dir != "." && !dirExists(dir) {
					out = append(out, c+": no directory "+dir)
				}
			}
		}
	}
	return out
}

func isDir(p string) bool {
	st, err := os.Stat(filepath.FromSlash(p))
	return err == nil && st.IsDir()
}

func TestReadmeCommandsExist(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatal(err)
	}
	cmds := commands(string(readme))
	if len(cmds) == 0 {
		t.Fatal("README.md has no make or go commands in code blocks; nothing to check")
	}
	for _, m := range missing(cmds, string(makefile), isDir) {
		t.Error(m)
	}
}

// The check itself must be able to fail.
func TestReadmeCheckCatchesMissing(t *testing.T) {
	readme := "```\n$ make test\nmake deploy\ngo run ./cmd/raftlockd --id n1\ngo run ./cmd/nope\ngo test ./internal/... -race\n```\nmake outside a block is prose\n"
	makefile := ".PHONY: test\n\ntest:\n\tgo test ./...\n"
	dirs := map[string]bool{"cmd/raftlockd": true, "internal": true}

	got := missing(commands(readme), makefile, func(d string) bool { return dirs[strings.TrimPrefix(d, "./")] })

	want := []string{"make deploy: no make target deploy", "go run ./cmd/nope: no directory ./cmd/nope"}
	if !slices.Equal(got, want) {
		t.Errorf("missing = %q, want %q", got, want)
	}
}
