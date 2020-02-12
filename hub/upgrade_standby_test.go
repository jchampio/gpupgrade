package hub_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpupgrade/hub"
)

func TestUpgradeStandby(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("it upgrades the standby through gpinitstandby", func(t *testing.T) {
		hub.ResetUpgradeStandby()

		spyEnv := NewSpyGreenplumEnv()

		config := hub.StandbyConfig{
			Port:          8888,
			Hostname:      "some-hostname",
			DataDirectory: "/some/standby/data/directory",
			GreenplumEnv:  spyEnv,
		}

		hub.UpgradeStandby(config)

		if spyEnv.TimesRunWasCalledWith("gpinitstandby") != 2 {
			t.Errorf("got %v calls to config.Run, wanted %v calls",
				spyEnv.TimesRunWasCalledWith("gpinitstandby"),
				2)
		}

		if !spyEnv.Call("gpinitstandby", 1).ArgumentsInclude("-r") {
			t.Errorf("expected remove to have been called")
		}

		portArgument := spyEnv.
			Call("gpinitstandby", 2).
			ArgumentValue("-P")

		hostnameArgument := spyEnv.
			Call("gpinitstandby", 2).
			ArgumentValue("-s")

		dataDirectoryArgument := spyEnv.
			Call("gpinitstandby", 2).
			ArgumentValue("-S")

		automaticArgument := spyEnv.
			Call("gpinitstandby", 2).
			ArgumentsInclude("-a")

		if portArgument != "8888" {
			t.Errorf("got port for new standby = %v, wanted %v",
				portArgument, "8888")
		}

		if hostnameArgument != "some-hostname" {
			t.Errorf("got hostname for new standby = %v, wanted %v",
				hostnameArgument, "some-hostname")
		}

		if dataDirectoryArgument != "/some/standby/data/directory" {
			t.Errorf("got standby data directory for new standby = %v, wanted %v",
				dataDirectoryArgument, "/some/standby/data/directory")
		}

		if !automaticArgument {
			t.Error("got automatic argument to be set, it was not")
		}
	})
}

func NewSpyGreenplumEnv() *spyGreenplumEnv {
	return &spyGreenplumEnv{
		calls: make(map[string][]*SpyCall),
	}
}

func (e *spyGreenplumEnv) BinDir() string {
	return ""
}

func (e *spyGreenplumEnv) MasterDataDirectory() string {
	return ""
}

func (e *spyGreenplumEnv) MasterPort() int {
	return 9999
}

type SpyCall struct {
	arguments []string
}

func (c *SpyCall) ArgumentsInclude(argName string) bool {
	for _, arg := range c.arguments {
		if argName == arg {
			return true
		}
	}
	return false
}

func (c *SpyCall) ArgumentValue(flag string) string {
	for i := 0; i < len(c.arguments)-1; i++ {
		current := c.arguments[i]
		next := c.arguments[i+1]

		if flag == current {
			return next
		}
	}

	return ""
}

func (e *spyGreenplumEnv) Run(utilityName string, arguments ...string) error {
	if e.calls == nil {
		e.calls = make(map[string][]*SpyCall)
	}

	calls := e.calls[utilityName]
	e.calls[utilityName] = append(calls, &SpyCall{arguments: arguments})

	return nil
}

type spyGreenplumEnv struct {
	calls map[string][]*SpyCall
}

func (e *spyGreenplumEnv) TimesRunWasCalledWith(utilityName string) int {
	return len(e.calls[utilityName])
}

func (e *spyGreenplumEnv) Call(utilityName string, nthCall int) *SpyCall {
	callsToUtility := e.calls[utilityName]

	if len(callsToUtility) == 0 {
		return &SpyCall{}
	}

	if len(callsToUtility) >= nthCall-1 {
		return callsToUtility[nthCall-1]
	}

	return &SpyCall{}
}
