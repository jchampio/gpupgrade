// +build debug

package step

import (
	"io/ioutil"

	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/idl"
)

type DoubleRunner struct {
	Err          error
	DoubleErrors []error

	streams OutStreams
}

func NewDoubleRunner(sender idl.MessageSender) *DoubleRunner {
	mux := newMultiplexedStream(sender, ioutil.Discard)
	return &DoubleRunner{streams: mux}
}

func (d *DoubleRunner) Run(substep idl.Substep, f func(OutStreams) error) {
	if d.Err != nil {
		return
	}

	err := f(d.streams)
	if err != nil {
		// First run should succeed.
		d.Err = xerrors.Errorf("substep %s: first run: %w", substep, err)
		return
	}

	err = f(d.streams)
	if err != nil {
		// Second run might not. Save it for later analysis.
		err = xerrors.Errorf("substep %s: second run: %w", substep, err)
		d.DoubleErrors = append(d.DoubleErrors, err)
	}
}
