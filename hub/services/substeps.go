package services

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus/file"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

func (h *Hub) BeginStep(name string, stream messageSender) (*SubstepChain, error) {
	// Create a log file to contain step output.
	path := filepath.Join(h.conf.StateDir, fmt.Sprintf("%s.log", name))
	log, err := utils.System.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, xerrors.Errorf(`step "%s": %w`, name, err)
	}

	_, err = fmt.Fprintf(log, "\n%s in progress.\n", strings.Title(name))
	if err != nil {
		log.Close()
		return nil, xerrors.Errorf(`logging step "%s": %w`, name, err)
	}

	// TODO move to global initialization
	dir := filepath.Join(h.conf.StateDir, "status")
	if err = utils.System.MkdirAll(dir, 0700); err != nil {
		log.Close()
		return nil, xerrors.Errorf(`creating status directory: %w`, err)
	}

	return &SubstepChain{
		name:   name,
		stream: newMultiplexedStream(stream, log),
		log:    log,
		dir:    dir,
	}, nil
}

type SubstepChain struct {
	name   string
	stream *multiplexedStream
	log    io.WriteCloser
	dir    string
	err    error
}

func (c *SubstepChain) Run(code idl.UpgradeSteps, f func(OutStreams) error) {
	if c.err != nil {
		// Short-circuit remaining elements in the chain.
		return
	}

	name := strings.ToLower(code.String())

	var err error
	defer func() {
		if err != nil {
			c.err = xerrors.Errorf(`substep "%s": %w`, name, err)
		}
	}()

	_, err = fmt.Fprintf(c.stream.writer, "\nStarting %s...\n\n", name)
	if err != nil {
		return
	}

	dir := filepath.Join(c.dir, strings.ToLower(code.String()))
	s := &substep{dir, code, c.stream.stream}

	err = s.MarkInProgress()
	if err != nil {
		if err == ErrSkip {
			// This is not an error condition; just short-circuit.
			err = nil
		}
		return
	}

	err = f(c.stream)
	if err != nil {
		s.MarkFailed()
		return
	}

	s.MarkComplete()
}

func (c *SubstepChain) Err() error {
	return c.err
}

func (c *SubstepChain) Finish() error {
	if err := c.log.Close(); err != nil {
		return xerrors.Errorf(`step "%s": %w`, c.name, err)
	}

	return nil
}

type substep struct {
	dir    string           // path to step-specific state directory
	code   idl.UpgradeSteps // the gRPC code associated with this step
	stream messageSender    // the stream on which to send status messages
}

func (s *substep) send(status idl.StepStatus) {
	// A stream is not guaranteed to remain connected during execution, so
	// errors are explicitly ignored.
	_ = s.stream.Send(&idl.Message{
		Contents: &idl.Message_Status{&idl.UpgradeStepStatus{
			Step:   s.code,
			Status: status,
		}},
	})
}

func touch(path string) error {
	f, err := utils.System.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	f.Close()
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)

	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

func (s *substep) failMarker() string {
	return filepath.Join(s.dir, "failed")
}

func (s *substep) doneMarker() string {
	return filepath.Join(s.dir, "complete")
}

var ErrSkip = errors.New("skipping completed substep")

func (s *substep) MarkInProgress() error {
	err := utils.System.Mkdir(s.dir, 0700)

	if os.IsExist(err) {
		// The step was already started; see if it failed or completed.
		var done, failed bool

		done, err = exists(s.doneMarker())
		if err != nil {
			return xerrors.Errorf("checking completion: %w", err)
		}

		if done {
			// We don't need to perform this step again.
			s.send(idl.StepStatus_SKIPPED)
			return ErrSkip
		}

		failed, err = exists(s.failMarker())
		if err != nil {
			return xerrors.Errorf("checking failure: %w", err)
		}

		if !failed {
			// Bad. Is this step already running elsewhere?
			return errors.New("step is already marked in-progress")
		}

		// This step previously failed; clean up and continue.
		err = os.Remove(s.failMarker())
	}

	if err != nil {
		return xerrors.Errorf(`marking in-progress: %w`, err)
	}

	s.send(idl.StepStatus_RUNNING)
	return nil
}

func (s *substep) MarkFailed() error {
	if err := touch(s.failMarker()); err != nil {
		return xerrors.Errorf(`marking failed: %w`, err)
	}

	s.send(idl.StepStatus_FAILED)
	return nil
}

func (s *substep) MarkComplete() error {
	if err := touch(s.doneMarker()); err != nil {
		return xerrors.Errorf(`marking complete: %w`, err)
	}

	s.send(idl.StepStatus_COMPLETE)
	return nil
}

// OutStreams collects the conceptual output and error streams into a single
// interface.
type OutStreams interface {
	Stdout() io.Writer
	Stderr() io.Writer
}

// Substep executes an upgrade substep of the given name using the provided
// implementation callback. All status and error reporting is coordinated on the
// provided stream.
func (h *Hub) Substep(stream *multiplexedStream, name string, f func(OutStreams) error) error {
	gplog.Info("starting %s", name)
	_, err := fmt.Fprintf(stream.writer, "\nStarting %s...\n\n", name)
	if err != nil {
		return xerrors.Errorf("failed writing to log: %w", err)
	}

	step, err := h.InitializeStep(name, stream.stream)
	if err != nil {
		gplog.Error(err.Error())
		return err
	}

	err = f(stream)
	if err != nil {
		gplog.Error(err.Error())
		step.MarkFailed()
	} else {
		step.MarkComplete()
	}

	return err
}

// Extracts common hub logic to reset state directory, mark step as in-progress,
// and control status streaming.
func (h *Hub) InitializeStep(step string, stream messageSender) (upgradestatus.StateWriter, error) {
	stepWriter := streamStepWriter{
		h.checklist.GetStepWriter(step),
		stream,
	}

	err := stepWriter.ResetStateDir()
	if err != nil {
		return nil, errors.Wrap(err, "failed to reset state directory")
	}

	err = stepWriter.MarkInProgress()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to set %s to %s", step, file.InProgress)
	}

	return stepWriter, nil
}

// streamStepWriter extends the standard StepWriter, which only writes state to
// disk, with functionality that sends status updates across the given stream.
// (In practice this stream will be a gRPC CliToHub_XxxServer interface.)
type streamStepWriter struct {
	upgradestatus.StateWriter
	stream messageSender
}

type messageSender interface {
	Send(*idl.Message) error // matches gRPC streaming Send()
}

func sendStatus(stream messageSender, step idl.UpgradeSteps, status idl.StepStatus) {
	// A stream is not guaranteed to remain connected during execution, so
	// errors are explicitly ignored.
	_ = stream.Send(&idl.Message{
		Contents: &idl.Message_Status{&idl.UpgradeStepStatus{
			Step:   step,
			Status: status,
		}},
	})
}

func (s streamStepWriter) MarkInProgress() error {
	if err := s.StateWriter.MarkInProgress(); err != nil {
		return err
	}

	sendStatus(s.stream, s.Code(), idl.StepStatus_RUNNING)
	return nil
}

func (s streamStepWriter) MarkComplete() error {
	if err := s.StateWriter.MarkComplete(); err != nil {
		return err
	}

	sendStatus(s.stream, s.Code(), idl.StepStatus_COMPLETE)
	return nil
}

func (s streamStepWriter) MarkFailed() error {
	if err := s.StateWriter.MarkFailed(); err != nil {
		return err
	}

	sendStatus(s.stream, s.Code(), idl.StepStatus_FAILED)
	return nil
}

// multiplexedStream provides an implementation of OutStreams that safely
// serializes any simultaneous writes to an underlying messageSender. A fallback
// io.Writer (in case the gRPC stream closes) also receives any output that is
// written to the streams.
type multiplexedStream struct {
	stream messageSender
	writer io.Writer
	mutex  sync.Mutex

	stdout io.Writer
	stderr io.Writer
}

func newMultiplexedStream(stream messageSender, writer io.Writer) *multiplexedStream {
	m := &multiplexedStream{
		stream: stream,
		writer: writer,
	}

	m.stdout = &streamWriter{
		multiplexedStream: m,
		cType:             idl.Chunk_STDOUT,
	}
	m.stderr = &streamWriter{
		multiplexedStream: m,
		cType:             idl.Chunk_STDERR,
	}

	return m
}

func (m *multiplexedStream) Stdout() io.Writer {
	return m.stdout
}

func (m *multiplexedStream) Stderr() io.Writer {
	return m.stderr
}

type streamWriter struct {
	*multiplexedStream
	cType idl.Chunk_Type
}

func (w *streamWriter) Write(p []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	n, err := w.writer.Write(p)
	if err != nil {
		return n, err
	}

	if w.stream != nil {
		// Attempt to send the chunk to the client. Since the client may close
		// the connection at any point, errors here are logged and otherwise
		// ignored. After the first send error, no more attempts are made.

		chunk := &idl.Chunk{
			Buffer: p,
			Type:   w.cType,
		}

		err = w.stream.Send(&idl.Message{
			Contents: &idl.Message_Chunk{chunk},
		})

		if err != nil {
			gplog.Info("halting client stream: %v", err)
			w.stream = nil
		}
	}

	return len(p), nil
}
