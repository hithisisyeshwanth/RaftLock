package raft

import "testing"

var (
	_ TimeoutSource = (*SeededTimeouts)(nil)
	_ TimeoutSource = (*FixedTimeouts)(nil)
)

func TestConfigDefaults(t *testing.T) {
	c := DefaultConfig()
	if c.ElectionTimeoutMin != 150 {
		t.Errorf("ElectionTimeoutMin = %d, want 150", c.ElectionTimeoutMin)
	}
	if c.ElectionTimeoutMax != 300 {
		t.Errorf("ElectionTimeoutMax = %d, want 300", c.ElectionTimeoutMax)
	}
	if c.HeartbeatInterval != 50 {
		t.Errorf("HeartbeatInterval = %d, want 50", c.HeartbeatInterval)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("DefaultConfig().Validate() = %v, want nil", err)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"defaults", Config{ElectionTimeoutMin: 150, ElectionTimeoutMax: 300, HeartbeatInterval: 50}, false},
		{"smallest valid", Config{ElectionTimeoutMin: 2, ElectionTimeoutMax: 3, HeartbeatInterval: 1}, false},
		{"min equals max", Config{ElectionTimeoutMin: 200, ElectionTimeoutMax: 200, HeartbeatInterval: 50}, true},
		{"min above max", Config{ElectionTimeoutMin: 300, ElectionTimeoutMax: 150, HeartbeatInterval: 50}, true},
		{"heartbeat equals min", Config{ElectionTimeoutMin: 150, ElectionTimeoutMax: 300, HeartbeatInterval: 150}, true},
		{"heartbeat above min", Config{ElectionTimeoutMin: 150, ElectionTimeoutMax: 300, HeartbeatInterval: 200}, true},
		{"heartbeat just below min", Config{ElectionTimeoutMin: 150, ElectionTimeoutMax: 300, HeartbeatInterval: 149}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() on %+v = %v, wantErr %v", tt.cfg, err, tt.wantErr)
			}
		})
	}
}

func TestConfigSeededTimeoutsReproducible(t *testing.T) {
	a := NewSeededTimeouts(42, 150, 300)
	b := NewSeededTimeouts(42, 150, 300)
	for i := 0; i < 200; i++ {
		if x, y := a.Next(), b.Next(); x != y {
			t.Fatalf("draw %d: same seed gave %d and %d", i, x, y)
		}
	}
}

func TestConfigSeededTimeoutsSeedMatters(t *testing.T) {
	a := NewSeededTimeouts(1, 150, 300)
	b := NewSeededTimeouts(2, 150, 300)
	for i := 0; i < 200; i++ {
		if a.Next() != b.Next() {
			return
		}
	}
	t.Error("seeds 1 and 2 produced identical sequences over 200 draws")
}

func TestConfigSeededTimeoutsInRange(t *testing.T) {
	tests := []struct{ min, max int }{
		{150, 300},
		{10, 12},
		{5, 6},
	}
	for _, tt := range tests {
		s := NewSeededTimeouts(7, tt.min, tt.max)
		seen := map[int]bool{}
		for i := 0; i < 2000; i++ {
			v := s.Next()
			if v < tt.min || v > tt.max {
				t.Fatalf("range [%d, %d]: draw %d = %d is out of range", tt.min, tt.max, i, v)
			}
			seen[v] = true
		}
		// Both bounds are inclusive, so a narrow range must hit every value.
		if tt.max-tt.min <= 2 && len(seen) != tt.max-tt.min+1 {
			t.Errorf("range [%d, %d]: saw %d distinct values, want %d (both bounds inclusive)",
				tt.min, tt.max, len(seen), tt.max-tt.min+1)
		}
	}
}

func TestConfigSeededTimeoutsVary(t *testing.T) {
	s := NewSeededTimeouts(3, 150, 300)
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		seen[s.Next()] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct timeouts in 200 draws, want them to vary", len(seen))
	}
}

func TestConfigFixedTimeouts(t *testing.T) {
	for _, v := range []int{1, 150, 300} {
		s := NewFixedTimeouts(v)
		for i := 0; i < 50; i++ {
			if got := s.Next(); got != v {
				t.Fatalf("NewFixedTimeouts(%d): draw %d = %d", v, i, got)
			}
		}
	}
}
