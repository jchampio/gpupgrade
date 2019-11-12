package services

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/onsi/gomega/gbytes"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
	"github.com/greenplum-db/gpupgrade/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMultiplexedStream(t *testing.T) {
	var log *gbytes.Buffer // contains gplog output

	// Store gplog output.
	_, _, log = testhelper.SetupTestLogger()

	t.Run("forwards stdout and stderr to the stream", func(t *testing.T) {
		g := NewGomegaWithT(t)

		// We can't rely on each write from the subprocess to result in exactly
		// one call to stream.Send(). Instead, concatenate the byte buffers as
		// they are sent and compare them at the end.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStream := mock_idl.NewMockCliToHub_ExecuteServer(ctrl)
		var stdout bytes.Buffer
		var stderr bytes.Buffer

		mockStream.EXPECT().
			Send(gomock.Any()).
			AnyTimes(). // Send will be called an indeterminate number of times

			DoAndReturn(func(msg *idl.Message) error {
				defer GinkgoRecover()

				var buf *bytes.Buffer
				c := msg.GetChunk()

				switch c.Type {
				case idl.Chunk_STDOUT:
					buf = &stdout
				case idl.Chunk_STDERR:
					buf = &stderr
				default:
					Fail("unexpected chunk type")
				}

				buf.Write(c.Buffer)
				return nil
			})

		stream := newMultiplexedStream(mockStream, ioutil.Discard)

		const (
			expectedStdout = "expected\nstdout\n"
			expectedStderr = "process\nstderr\n"
		)
		fmt.Fprint(stream.Stdout(), expectedStdout)
		fmt.Fprint(stream.Stderr(), expectedStderr)

		g.Expect(stdout.String()).To(Equal(expectedStdout))
		g.Expect(stderr.String()).To(Equal(expectedStderr))
	})

	t.Run("also writes all data to a local io.Writer", func(t *testing.T) {
		g := NewGomegaWithT(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStream := mock_idl.NewMockCliToHub_ExecuteServer(ctrl)
		mockStream.EXPECT().
			Send(gomock.Any()).
			AnyTimes()

		var buf bytes.Buffer
		stream := newMultiplexedStream(mockStream, &buf)

		// Write 10 bytes to each stream.
		for i := 0; i < 10; i++ {
			stream.Stdout().Write([]byte{'O'})
			stream.Stderr().Write([]byte{'E'})
		}

		// Stdout and stderr are not guaranteed to interleave in any particular
		// order. Just count the number of bytes in each that we see (there
		// should be exactly ten).
		numO := 0
		numE := 0
		for _, b := range buf.Bytes() {
			switch b {
			case 'O':
				numO++
			case 'E':
				numE++
			default:
				Fail(fmt.Sprintf("unexpected byte %#v in output %#v", b, buf.String()))
			}
		}

		g.Expect(numO).To(Equal(10))
		g.Expect(numE).To(Equal(10))
	})

	t.Run("continues writing to the local io.Writer even if Send fails", func(t *testing.T) {
		g := NewGomegaWithT(t)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Return an error during Send.
		mockStream := mock_idl.NewMockCliToHub_ExecuteServer(ctrl)
		mockStream.EXPECT().
			Send(gomock.Any()).
			Return(errors.New("error during send")).
			Times(1) // we expect only one failed attempt to Send

		var buf bytes.Buffer
		stream := newMultiplexedStream(mockStream, &buf)

		// Write 10 bytes to each stream.
		for i := 0; i < 10; i++ {
			stream.Stdout().Write([]byte{'O'})
			stream.Stderr().Write([]byte{'E'})
		}

		// The Writer should not have been affected in any way.
		g.Expect(buf.Bytes()).To(HaveLen(20))
		g.Expect(log).To(gbytes.Say("halting client stream: error during send"))
	})
}

func TestSubstep(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testhelper.SetupTestLogger()

	cm := testutils.NewMockChecklistManager()
	hub := NewHub(nil, nil, nil, nil, cm)

	sender := mock_idl.NewMockCliToHub_ExecuteServer(ctrl)
	sender.EXPECT().
		Send(gomock.Any()).
		AnyTimes()

	stream := &multiplexedStream{
		stream: sender,
		writer: new(bytes.Buffer),
	}

	expected := errors.New("ahhhh")
	err := hub.Substep(stream, "my substep",
		func(_ OutStreams) error {
			return expected
		})

	if !xerrors.Is(err, expected) {
		t.Errorf("returned %#v, want %#v", err, expected)
	}
}
