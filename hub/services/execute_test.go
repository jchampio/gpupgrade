package services

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	"github.com/greenplum-db/gpupgrade/utils"

	. "github.com/onsi/gomega"
)

const StreamingMainStdout = "expected\nstdout\n"
const StreamingMainStderr = "process\nstderr\n"

// Streams the above stdout/err constants to the corresponding standard file
// descriptors, alternately interleaving five-byte chunks.
func StreamingMain() {
	stdout := bytes.NewBufferString(StreamingMainStdout)
	stderr := bytes.NewBufferString(StreamingMainStderr)

	for stdout.Len() > 0 || stderr.Len() > 0 {
		os.Stdout.Write(stdout.Next(5))
		os.Stderr.Write(stderr.Next(5))
	}
}

// Writes to stdout and ignores any failure to do so.
func BlindlyWritingMain() {
	// Ignore SIGPIPE. Note that the obvious signal.Ignore(syscall.SIGPIPE)
	// doesn't work as expected; see https://github.com/golang/go/issues/32386.
	signal.Notify(make(chan os.Signal), syscall.SIGPIPE)

	fmt.Println("blah blah blah blah")
	fmt.Println("blah blah blah blah")
	fmt.Println("blah blah blah blah")
}

func init() {
	exectest.RegisterMains(
		StreamingMain,
		BlindlyWritingMain,
	)
}

func TestStreaming(t *testing.T) {
	g := NewGomegaWithT(t)

	var pair clusterPair // the unit under test

	// Disable exec.Command. This way, if a test forgets to mock it out, we
	// crash the test instead of executing code on a dev system.
	execCommand = nil

	testhelper.SetupTestLogger()

	// Initialize the sample cluster pair.
	pair = clusterPair{
		Source: &utils.Cluster{
			BinDir: "/old/bin",
			Cluster: &cluster.Cluster{
				ContentIDs: []int{-1},
				Segments: map[int]cluster.SegConfig{
					-1: cluster.SegConfig{
						Port:    5432,
						DataDir: "/data/old",
					},
				},
			},
		},
		Target: &utils.Cluster{
			BinDir: "/new/bin",
			Cluster: &cluster.Cluster{
				ContentIDs: []int{-1},
				Segments: map[int]cluster.SegConfig{
					-1: cluster.SegConfig{
						Port:    5433,
						DataDir: "/data/new",
					},
				},
			},
		},
	}

	t.Run("streams stdout and stderr to the client", func(t *testing.T) {
		stream := new(bufferedStreams)
		execCommand = exectest.NewCommand(StreamingMain)

		err := pair.ConvertMaster(stream, "", false)
		g.Expect(err).NotTo(HaveOccurred())

		g.Expect(stream.stdout.String()).To(Equal(StreamingMainStdout))
		g.Expect(stream.stderr.String()).To(Equal(StreamingMainStderr))
	})

	t.Run("returns an error if the command succeeds but the io.Writer fails", func(t *testing.T) {
		// Don't fail in the subprocess even when the stdout stream is closed.
		execCommand = exectest.NewCommand(BlindlyWritingMain)

		expectedErr := errors.New("write failed!")
		err := pair.ConvertMaster(failingStreams{expectedErr}, "", false)

		g.Expect(err).To(Equal(expectedErr))
	})
}

// bufferedStreams is an implementation of OutStreams that just writes to
// bytes.Buffers.
type bufferedStreams struct {
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func (b *bufferedStreams) Stdout() io.Writer {
	return &b.stdout
}

func (b *bufferedStreams) Stderr() io.Writer {
	return &b.stderr
}

// failingStreams is an implementation of OutStreams for which every call to a
// stream's Write() method will fail with the given error.
type failingStreams struct {
	err error
}

func (f failingStreams) Stdout() io.Writer {
	return &failingWriter{f.err}
}

func (f failingStreams) Stderr() io.Writer {
	return &failingWriter{f.err}
}
