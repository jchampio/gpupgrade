package exectest

// SetRunCalled allows tests to change the value of the internal runCalled flag.
// (See the tests for why this is useful.)
func SetRunCalled(called bool) {
	runCalled = called
}
