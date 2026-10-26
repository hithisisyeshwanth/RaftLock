package check

import "fmt"

// LogMatching checks Figure 3's Log Matching: if two logs have an entry with the same
// index and term, the logs are identical up to that index. It keeps no state.
type LogMatching struct{}

func (LogMatching) Observe(snaps []Snapshot) error {
	for i, a := range snaps {
		for _, b := range snaps[i+1:] {
			if err := matchPair(a, b); err != nil {
				return err
			}
		}
	}
	return nil
}

func matchPair(a, b Snapshot) error {
	diverged := false // an earlier index held different terms
	for i := 0; i < len(a.Log) && i < len(b.Log); i++ {
		x, y := a.Log[i], b.Log[i]
		if x.Term != y.Term {
			diverged = true
		} else if diverged || !x.Equal(y) {
			return fmt.Errorf("log matching violated: %s and %s share index %d term %d but their logs differ at or before it", a.ID, b.ID, i+1, x.Term)
		}
	}
	return nil
}
