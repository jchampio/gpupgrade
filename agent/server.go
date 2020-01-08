package agent

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	zipkin "github.com/openzipkin/zipkin-go"
	zipkingrpc "github.com/openzipkin/zipkin-go/middleware/grpc"
	zipkinhttp "github.com/openzipkin/zipkin-go/reporter/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils/daemon"
	"github.com/greenplum-db/gpupgrade/utils/log"
)

type Server struct {
	executor cluster.Executor
	conf     Config

	mu      sync.Mutex
	server  *grpc.Server
	lis     net.Listener
	stopped chan struct{}
	daemon  bool
}

type Config struct {
	Port     int
	StateDir string
}

func NewServer(executor cluster.Executor, conf Config) *Server {
	return &Server{
		executor: executor,
		conf:     conf,
		stopped:  make(chan struct{}, 1),
	}
}

// MakeDaemon tells the Server to disconnect its stdout/stderr streams after
// successfully starting up.
func (a *Server) MakeDaemon() {
	a.daemon = true
}

func (a *Server) Start() {
	createIfNotExists(a.conf.StateDir)
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(a.conf.Port))
	if err != nil {
		gplog.Fatal(err, "failed to listen")
	}

	// Set up Zipkin tracing.
	reporter := zipkinhttp.NewReporter("http://localhost:9411/api/v2/spans")
	defer reporter.Close()

	endpoint, err := zipkin.NewEndpoint("agent", lis.Addr().String())
	if err != nil {
		gplog.Fatal(err, "failed to create zipkin endpoint")
	}

	tracer, err := zipkin.NewTracer(
		reporter,
		zipkin.WithSampler(zipkin.AlwaysSample),
		zipkin.WithLocalEndpoint(endpoint),
	)
	if err != nil {
		gplog.Fatal(err, "failed to create zipkin tracer")
	}

	// Set up the global tracer instance.
	agentTracer = cmdTracer{tracer}

	// Set up an interceptor function to log any panics we get from request
	// handlers.
	interceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer log.WritePanics()
		return handler(ctx, req)
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor),
		grpc.StatsHandler(zipkingrpc.NewServerHandler(tracer)),
	)

	a.mu.Lock()
	a.server = server
	a.lis = lis
	a.mu.Unlock()

	idl.RegisterAgentServer(server, a)
	reflection.Register(server)

	if a.daemon {
		// Send an identifier string back to the hub, and log it locally for
		// easier debugging.
		info := fmt.Sprintf("Agent started on port %d (pid %d)", a.conf.Port, os.Getpid())

		fmt.Println(info)
		daemon.Daemonize()
		gplog.Info(info)
	}

	err = server.Serve(lis)
	if err != nil {
		gplog.Fatal(err, "failed to serve: %s", err)
	}

	a.stopped <- struct{}{}
}

func (a *Server) StopAgent(ctx context.Context, in *idl.StopAgentRequest) (*idl.StopAgentReply, error) {
	a.Stop()
	return &idl.StopAgentReply{}, nil
}

func (a *Server) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.server != nil {
		a.server.Stop()
		<-a.stopped
	}
}

func createIfNotExists(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0777)
	}
}

type cmdTracer struct {
	*zipkin.Tracer
}

var agentTracer cmdTracer

func (c cmdTracer) Output(ctx context.Context, cmd *exec.Cmd) ([]byte, error) {
	if c.Tracer != nil {
		span, _ := c.StartSpanFromContext(ctx, cmd.Path,
			zipkin.Tags(map[string]string{
				"command": strings.Join(append([]string{cmd.Path}, cmd.Args...), " "),
			}),
		)
		defer span.Finish()
	}

	return cmd.Output()
}
