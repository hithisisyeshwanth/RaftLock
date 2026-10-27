package raft

import "testing"

// Section 5.6: safety never depends on timing, but availability does. Followers must hear several
// heartbeats within one election timeout, or a healthy leader gets deposed by a single slow or lost
// message. The rule here: the election timeout minimum is at least 5 heartbeat intervals.
func TestConfigTimingElectionMinAtLeastFiveHeartbeats(t *testing.T) {
	tests := []struct {
		name    string
		min     int
		hb      int
		wantErr bool
	}{
		{"well above 5x", 300, 50, false},
		{"exactly 5x", 250, 50, false},
		{"just below 5x", 249, 50, true},
		{"3x", 150, 50, true},
		{"exactly 5x, small numbers", 5, 1, false},
		{"just below 5x, small numbers", 4, 1, true},
		{"exactly 5x, heartbeat 30", 150, 30, false},
		{"just below 5x, heartbeat 30", 149, 30, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{ElectionTimeoutMin: tt.min, ElectionTimeoutMax: 2 * tt.min, HeartbeatInterval: tt.hb}
			if err := cfg.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate(min %d, max %d, heartbeat %d) = %v, wantErr %v", tt.min, 2*tt.min, tt.hb, err, tt.wantErr)
			}
		})
	}
}

// The earlier rules still hold alongside the new one.
func TestConfigTimingEarlierRulesStillApply(t *testing.T) {
	for _, cfg := range []Config{
		{ElectionTimeoutMin: 500, ElectionTimeoutMax: 500, HeartbeatInterval: 50}, // min == max
		{ElectionTimeoutMin: 600, ElectionTimeoutMax: 500, HeartbeatInterval: 50}, // min > max
	} {
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate(%+v) = nil, want an error", cfg)
		}
	}
}
