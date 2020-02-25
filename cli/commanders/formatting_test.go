package commanders

import (
	"testing"

	"github.com/greenplum-db/gpupgrade/idl"
)

func TestFormatStatus(t *testing.T) {
	t.Run("it formats all possible types", func(t *testing.T) {
		ignoreUnknownStep := 1
		numberOfSubsteps := len(idl.Substep_name) - ignoreUnknownStep

		if numberOfSubsteps != len(lines) {
			t.Errorf("got %q, expected FormatStatus to be able to format all %d statuses %q. Formatted only %d",
				lines, len(idl.Substep_name), idl.Substep_name, len(lines))
		}
	})
}
