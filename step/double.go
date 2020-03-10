// +build debug

package step

import (
	"io/ioutil"

	"github.com/greenplum-db/gpupgrade/idl"
)

type DoubleRunner struct {
	Err error

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

	d.Err = f(d.streams)
	if d.Err != nil {
		return
	}

	d.Err = f(d.streams)
}
