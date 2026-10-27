package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// BenchmarkFileStorageSave measures one durable Save: write a temp file, fsync it, rename it over
// the state file, fsync the directory. Every vote, term change and log append pays this before a
// reply can leave, so it sets a floor on election timeouts (see raft.Config). Each Save rewrites
// the whole state, so the cost grows with the log.
//
//	go test ./internal/storage -run '^$' -bench Save
func BenchmarkFileStorageSave(b *testing.B) {
	for _, entries := range []int{0, 100, 1000} {
		b.Run(fmt.Sprintf("%d entries", entries), func(b *testing.B) {
			st := raft.PersistentState{CurrentTerm: 7, VotedFor: "n2"}
			for i := 1; i <= entries; i++ {
				st.Log = append(st.Log, raft.Entry{Term: 7, Index: raft.Index(i), Cmd: []byte(`{"Kind":"acquire","Lock":"jobs","ClientID":"c1","Seq":1}`)})
			}
			s := NewFileStorage(filepath.Join(b.TempDir(), "state"))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Save(st); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
