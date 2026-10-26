package check

type checker interface {
	Observe([]Snapshot) error
}

// All runs the five Figure 3 checkers on every observation and reports the first violation.
type All struct {
	checkers []checker
}

func NewAll() *All {
	return &All{checkers: []checker{
		NewElectionSafety(),
		LogMatching{},
		NewStateMachineSafety(),
		NewLeaderAppendOnly(),
		NewLeaderCompleteness(),
	}}
}

func (a *All) Observe(snaps []Snapshot) error {
	for _, c := range a.checkers {
		if err := c.Observe(snaps); err != nil {
			return err
		}
	}
	return nil
}
