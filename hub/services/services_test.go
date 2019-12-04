package services

import (
	"io"
	"io/ioutil"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

// Set it to nil so we don't accidentally execute a command for real during tests
func init() {
	ResetExecCommand()
}

func SetExecCommand(cmdFunc exectest.Command) {
	execCommand = cmdFunc
}

func ResetExecCommand() {
	execCommand = nil
}

// DevNull implements OutStreams by just discarding all writes.
var DevNull = devNull{}

type devNull struct{}

func (_ devNull) Stdout() io.Writer {
	return ioutil.Discard
}

func (_ devNull) Stderr() io.Writer {
	return ioutil.Discard
}

// failingWriter is an io.Writer for which all calls to Write() return an error.
type failingWriter struct {
	err error
}

func (f *failingWriter) Write(_ []byte) (int, error) {
	return 0, f.err
}
