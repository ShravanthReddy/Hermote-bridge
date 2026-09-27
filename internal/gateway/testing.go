package gateway

import "strconv"

// ForceReadyForTest marks the supervisor ready on the given port without
// running a child. Tests point it at a fake gateway.
func ForceReadyForTest(s *Supervisor, port string) {
	p, _ := strconv.Atoi(port)
	s.setState(StateReady, p)
}

// ForceStateForTest sets a state other than ready without running a child,
// so a fixture can hold the bridge in its gateway-dial loop, which reports
// each state to phones in `ctl gateway`.
func ForceStateForTest(s *Supervisor, state State) {
	s.setState(state, 0)
}
