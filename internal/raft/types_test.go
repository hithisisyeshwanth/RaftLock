package raft

import "testing"

func TestTypesRoleString(t *testing.T) {
	tests := []struct {
		role Role
		want string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.role.String(); got != tt.want {
				t.Errorf("Role.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTypesEntryEqual(t *testing.T) {
	base := Entry{Term: 2, Index: 5, Cmd: []byte("set x")}
	tests := []struct {
		name string
		a, b Entry
		want bool
	}{
		{"identical", base, Entry{Term: 2, Index: 5, Cmd: []byte("set x")}, true},
		{"different term", base, Entry{Term: 3, Index: 5, Cmd: []byte("set x")}, false},
		{"different index", base, Entry{Term: 2, Index: 6, Cmd: []byte("set x")}, false},
		{"different cmd bytes", base, Entry{Term: 2, Index: 5, Cmd: []byte("set y")}, false},
		{"cmd prefix", base, Entry{Term: 2, Index: 5, Cmd: []byte("set")}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Equal(tt.b); got != tt.want {
				t.Errorf("a.Equal(b) = %v, want %v", got, tt.want)
			}
			if got := tt.b.Equal(tt.a); got != tt.want {
				t.Errorf("b.Equal(a) = %v, want %v (Equal must be symmetric)", got, tt.want)
			}
		})
	}
}

func TestTypesZeroValues(t *testing.T) {
	var term Term
	if term != 0 {
		t.Errorf("zero Term = %d, want 0", term)
	}
	var index Index
	if index != 0 {
		t.Errorf("zero Index = %d, want 0", index)
	}
	var id NodeID
	if id != "" {
		t.Errorf("zero NodeID = %q, want empty", id)
	}
	var e Entry
	if e.Term != 0 || e.Index != 0 || len(e.Cmd) != 0 {
		t.Errorf("zero Entry = %+v, want Term 0, Index 0, empty Cmd", e)
	}
}
