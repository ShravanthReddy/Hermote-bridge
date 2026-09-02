package gateway

import "strconv"

// ForceReadyForTest marks the supervisor ready on the given port without
// running a child. Tests point it at a fake gateway.
func ForceReadyForTest(s *Supervisor, port string) {
	p, _ := strconv.Atoi(port)
	s.setState(StateReady, p)
}
