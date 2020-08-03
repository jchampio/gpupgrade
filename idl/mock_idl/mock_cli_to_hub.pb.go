// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/greenplum-db/gpupgrade/idl (interfaces: CliToHubClient,CliToHubServer,CliToHub_ExecuteServer,CliToHub_ExecuteClient)

// Package mock_idl is a generated GoMock package.
package mock_idl

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	idl "github.com/greenplum-db/gpupgrade/idl"
	grpc "google.golang.org/grpc"
	metadata "google.golang.org/grpc/metadata"
	reflect "reflect"
)

// MockCliToHubClient is a mock of CliToHubClient interface
type MockCliToHubClient struct {
	ctrl     *gomock.Controller
	recorder *MockCliToHubClientMockRecorder
}

// MockCliToHubClientMockRecorder is the mock recorder for MockCliToHubClient
type MockCliToHubClientMockRecorder struct {
	mock *MockCliToHubClient
}

// NewMockCliToHubClient creates a new mock instance
func NewMockCliToHubClient(ctrl *gomock.Controller) *MockCliToHubClient {
	mock := &MockCliToHubClient{ctrl: ctrl}
	mock.recorder = &MockCliToHubClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockCliToHubClient) EXPECT() *MockCliToHubClientMockRecorder {
	return m.recorder
}

// CheckDiskSpace mocks base method
func (m *MockCliToHubClient) CheckDiskSpace(arg0 context.Context, arg1 *idl.CheckDiskSpaceRequest, arg2 ...grpc.CallOption) (*idl.CheckDiskSpaceReply, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CheckDiskSpace", varargs...)
	ret0, _ := ret[0].(*idl.CheckDiskSpaceReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckDiskSpace indicates an expected call of CheckDiskSpace
func (mr *MockCliToHubClientMockRecorder) CheckDiskSpace(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckDiskSpace", reflect.TypeOf((*MockCliToHubClient)(nil).CheckDiskSpace), varargs...)
}

// Execute mocks base method
func (m *MockCliToHubClient) Execute(arg0 context.Context, arg1 *idl.ExecuteRequest, arg2 ...grpc.CallOption) (idl.CliToHub_ExecuteClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Execute", varargs...)
	ret0, _ := ret[0].(idl.CliToHub_ExecuteClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Execute indicates an expected call of Execute
func (mr *MockCliToHubClientMockRecorder) Execute(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockCliToHubClient)(nil).Execute), varargs...)
}

// Finalize mocks base method
func (m *MockCliToHubClient) Finalize(arg0 context.Context, arg1 *idl.FinalizeRequest, arg2 ...grpc.CallOption) (idl.CliToHub_FinalizeClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Finalize", varargs...)
	ret0, _ := ret[0].(idl.CliToHub_FinalizeClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Finalize indicates an expected call of Finalize
func (mr *MockCliToHubClientMockRecorder) Finalize(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Finalize", reflect.TypeOf((*MockCliToHubClient)(nil).Finalize), varargs...)
}

// GetConfig mocks base method
func (m *MockCliToHubClient) GetConfig(arg0 context.Context, arg1 *idl.GetConfigRequest, arg2 ...grpc.CallOption) (*idl.GetConfigReply, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetConfig", varargs...)
	ret0, _ := ret[0].(*idl.GetConfigReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetConfig indicates an expected call of GetConfig
func (mr *MockCliToHubClientMockRecorder) GetConfig(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConfig", reflect.TypeOf((*MockCliToHubClient)(nil).GetConfig), varargs...)
}

// Initialize mocks base method
func (m *MockCliToHubClient) Initialize(arg0 context.Context, arg1 *idl.InitializeRequest, arg2 ...grpc.CallOption) (idl.CliToHub_InitializeClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Initialize", varargs...)
	ret0, _ := ret[0].(idl.CliToHub_InitializeClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Initialize indicates an expected call of Initialize
func (mr *MockCliToHubClientMockRecorder) Initialize(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Initialize", reflect.TypeOf((*MockCliToHubClient)(nil).Initialize), varargs...)
}

// InitializeCreateCluster mocks base method
func (m *MockCliToHubClient) InitializeCreateCluster(arg0 context.Context, arg1 *idl.InitializeCreateClusterRequest, arg2 ...grpc.CallOption) (idl.CliToHub_InitializeCreateClusterClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "InitializeCreateCluster", varargs...)
	ret0, _ := ret[0].(idl.CliToHub_InitializeCreateClusterClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InitializeCreateCluster indicates an expected call of InitializeCreateCluster
func (mr *MockCliToHubClientMockRecorder) InitializeCreateCluster(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitializeCreateCluster", reflect.TypeOf((*MockCliToHubClient)(nil).InitializeCreateCluster), varargs...)
}

// RestartAgents mocks base method
func (m *MockCliToHubClient) RestartAgents(arg0 context.Context, arg1 *idl.RestartAgentsRequest, arg2 ...grpc.CallOption) (*idl.RestartAgentsReply, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RestartAgents", varargs...)
	ret0, _ := ret[0].(*idl.RestartAgentsReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RestartAgents indicates an expected call of RestartAgents
func (mr *MockCliToHubClientMockRecorder) RestartAgents(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RestartAgents", reflect.TypeOf((*MockCliToHubClient)(nil).RestartAgents), varargs...)
}

// Revert mocks base method
func (m *MockCliToHubClient) Revert(arg0 context.Context, arg1 *idl.RevertRequest, arg2 ...grpc.CallOption) (idl.CliToHub_RevertClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Revert", varargs...)
	ret0, _ := ret[0].(idl.CliToHub_RevertClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Revert indicates an expected call of Revert
func (mr *MockCliToHubClientMockRecorder) Revert(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Revert", reflect.TypeOf((*MockCliToHubClient)(nil).Revert), varargs...)
}

// StopServices mocks base method
func (m *MockCliToHubClient) StopServices(arg0 context.Context, arg1 *idl.StopServicesRequest, arg2 ...grpc.CallOption) (*idl.StopServicesReply, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StopServices", varargs...)
	ret0, _ := ret[0].(*idl.StopServicesReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StopServices indicates an expected call of StopServices
func (mr *MockCliToHubClientMockRecorder) StopServices(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StopServices", reflect.TypeOf((*MockCliToHubClient)(nil).StopServices), varargs...)
}

// MockCliToHubServer is a mock of CliToHubServer interface
type MockCliToHubServer struct {
	ctrl     *gomock.Controller
	recorder *MockCliToHubServerMockRecorder
}

// MockCliToHubServerMockRecorder is the mock recorder for MockCliToHubServer
type MockCliToHubServerMockRecorder struct {
	mock *MockCliToHubServer
}

// NewMockCliToHubServer creates a new mock instance
func NewMockCliToHubServer(ctrl *gomock.Controller) *MockCliToHubServer {
	mock := &MockCliToHubServer{ctrl: ctrl}
	mock.recorder = &MockCliToHubServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockCliToHubServer) EXPECT() *MockCliToHubServerMockRecorder {
	return m.recorder
}

// CheckDiskSpace mocks base method
func (m *MockCliToHubServer) CheckDiskSpace(arg0 context.Context, arg1 *idl.CheckDiskSpaceRequest) (*idl.CheckDiskSpaceReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckDiskSpace", arg0, arg1)
	ret0, _ := ret[0].(*idl.CheckDiskSpaceReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckDiskSpace indicates an expected call of CheckDiskSpace
func (mr *MockCliToHubServerMockRecorder) CheckDiskSpace(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckDiskSpace", reflect.TypeOf((*MockCliToHubServer)(nil).CheckDiskSpace), arg0, arg1)
}

// Execute mocks base method
func (m *MockCliToHubServer) Execute(arg0 *idl.ExecuteRequest, arg1 idl.CliToHub_ExecuteServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute
func (mr *MockCliToHubServerMockRecorder) Execute(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockCliToHubServer)(nil).Execute), arg0, arg1)
}

// Finalize mocks base method
func (m *MockCliToHubServer) Finalize(arg0 *idl.FinalizeRequest, arg1 idl.CliToHub_FinalizeServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Finalize", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Finalize indicates an expected call of Finalize
func (mr *MockCliToHubServerMockRecorder) Finalize(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Finalize", reflect.TypeOf((*MockCliToHubServer)(nil).Finalize), arg0, arg1)
}

// GetConfig mocks base method
func (m *MockCliToHubServer) GetConfig(arg0 context.Context, arg1 *idl.GetConfigRequest) (*idl.GetConfigReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConfig", arg0, arg1)
	ret0, _ := ret[0].(*idl.GetConfigReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetConfig indicates an expected call of GetConfig
func (mr *MockCliToHubServerMockRecorder) GetConfig(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConfig", reflect.TypeOf((*MockCliToHubServer)(nil).GetConfig), arg0, arg1)
}

// Initialize mocks base method
func (m *MockCliToHubServer) Initialize(arg0 *idl.InitializeRequest, arg1 idl.CliToHub_InitializeServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Initialize", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Initialize indicates an expected call of Initialize
func (mr *MockCliToHubServerMockRecorder) Initialize(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Initialize", reflect.TypeOf((*MockCliToHubServer)(nil).Initialize), arg0, arg1)
}

// InitializeCreateCluster mocks base method
func (m *MockCliToHubServer) InitializeCreateCluster(arg0 *idl.InitializeCreateClusterRequest, arg1 idl.CliToHub_InitializeCreateClusterServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InitializeCreateCluster", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// InitializeCreateCluster indicates an expected call of InitializeCreateCluster
func (mr *MockCliToHubServerMockRecorder) InitializeCreateCluster(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitializeCreateCluster", reflect.TypeOf((*MockCliToHubServer)(nil).InitializeCreateCluster), arg0, arg1)
}

// RestartAgents mocks base method
func (m *MockCliToHubServer) RestartAgents(arg0 context.Context, arg1 *idl.RestartAgentsRequest) (*idl.RestartAgentsReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RestartAgents", arg0, arg1)
	ret0, _ := ret[0].(*idl.RestartAgentsReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RestartAgents indicates an expected call of RestartAgents
func (mr *MockCliToHubServerMockRecorder) RestartAgents(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RestartAgents", reflect.TypeOf((*MockCliToHubServer)(nil).RestartAgents), arg0, arg1)
}

// Revert mocks base method
func (m *MockCliToHubServer) Revert(arg0 *idl.RevertRequest, arg1 idl.CliToHub_RevertServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Revert", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Revert indicates an expected call of Revert
func (mr *MockCliToHubServerMockRecorder) Revert(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Revert", reflect.TypeOf((*MockCliToHubServer)(nil).Revert), arg0, arg1)
}

// StopServices mocks base method
func (m *MockCliToHubServer) StopServices(arg0 context.Context, arg1 *idl.StopServicesRequest) (*idl.StopServicesReply, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StopServices", arg0, arg1)
	ret0, _ := ret[0].(*idl.StopServicesReply)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StopServices indicates an expected call of StopServices
func (mr *MockCliToHubServerMockRecorder) StopServices(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StopServices", reflect.TypeOf((*MockCliToHubServer)(nil).StopServices), arg0, arg1)
}

// MockCliToHub_ExecuteServer is a mock of CliToHub_ExecuteServer interface
type MockCliToHub_ExecuteServer struct {
	ctrl     *gomock.Controller
	recorder *MockCliToHub_ExecuteServerMockRecorder
}

// MockCliToHub_ExecuteServerMockRecorder is the mock recorder for MockCliToHub_ExecuteServer
type MockCliToHub_ExecuteServerMockRecorder struct {
	mock *MockCliToHub_ExecuteServer
}

// NewMockCliToHub_ExecuteServer creates a new mock instance
func NewMockCliToHub_ExecuteServer(ctrl *gomock.Controller) *MockCliToHub_ExecuteServer {
	mock := &MockCliToHub_ExecuteServer{ctrl: ctrl}
	mock.recorder = &MockCliToHub_ExecuteServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockCliToHub_ExecuteServer) EXPECT() *MockCliToHub_ExecuteServerMockRecorder {
	return m.recorder
}

// Context mocks base method
func (m *MockCliToHub_ExecuteServer) Context() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context
func (mr *MockCliToHub_ExecuteServerMockRecorder) Context() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockCliToHub_ExecuteServer)(nil).Context))
}

// RecvMsg mocks base method
func (m *MockCliToHub_ExecuteServer) RecvMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecvMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg
func (mr *MockCliToHub_ExecuteServerMockRecorder) RecvMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockCliToHub_ExecuteServer)(nil).RecvMsg), arg0)
}

// Send mocks base method
func (m *MockCliToHub_ExecuteServer) Send(arg0 *idl.Message) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send
func (mr *MockCliToHub_ExecuteServerMockRecorder) Send(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockCliToHub_ExecuteServer)(nil).Send), arg0)
}

// SendHeader mocks base method
func (m *MockCliToHub_ExecuteServer) SendHeader(arg0 metadata.MD) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendHeader indicates an expected call of SendHeader
func (mr *MockCliToHub_ExecuteServerMockRecorder) SendHeader(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendHeader", reflect.TypeOf((*MockCliToHub_ExecuteServer)(nil).SendHeader), arg0)
}

// SendMsg mocks base method
func (m *MockCliToHub_ExecuteServer) SendMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg
func (mr *MockCliToHub_ExecuteServerMockRecorder) SendMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockCliToHub_ExecuteServer)(nil).SendMsg), arg0)
}

// SetHeader mocks base method
func (m *MockCliToHub_ExecuteServer) SetHeader(arg0 metadata.MD) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetHeader indicates an expected call of SetHeader
func (mr *MockCliToHub_ExecuteServerMockRecorder) SetHeader(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetHeader", reflect.TypeOf((*MockCliToHub_ExecuteServer)(nil).SetHeader), arg0)
}

// SetTrailer mocks base method
func (m *MockCliToHub_ExecuteServer) SetTrailer(arg0 metadata.MD) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTrailer", arg0)
}

// SetTrailer indicates an expected call of SetTrailer
func (mr *MockCliToHub_ExecuteServerMockRecorder) SetTrailer(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTrailer", reflect.TypeOf((*MockCliToHub_ExecuteServer)(nil).SetTrailer), arg0)
}

// MockCliToHub_ExecuteClient is a mock of CliToHub_ExecuteClient interface
type MockCliToHub_ExecuteClient struct {
	ctrl     *gomock.Controller
	recorder *MockCliToHub_ExecuteClientMockRecorder
}

// MockCliToHub_ExecuteClientMockRecorder is the mock recorder for MockCliToHub_ExecuteClient
type MockCliToHub_ExecuteClientMockRecorder struct {
	mock *MockCliToHub_ExecuteClient
}

// NewMockCliToHub_ExecuteClient creates a new mock instance
func NewMockCliToHub_ExecuteClient(ctrl *gomock.Controller) *MockCliToHub_ExecuteClient {
	mock := &MockCliToHub_ExecuteClient{ctrl: ctrl}
	mock.recorder = &MockCliToHub_ExecuteClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockCliToHub_ExecuteClient) EXPECT() *MockCliToHub_ExecuteClientMockRecorder {
	return m.recorder
}

// CloseSend mocks base method
func (m *MockCliToHub_ExecuteClient) CloseSend() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseSend")
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseSend indicates an expected call of CloseSend
func (mr *MockCliToHub_ExecuteClientMockRecorder) CloseSend() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseSend", reflect.TypeOf((*MockCliToHub_ExecuteClient)(nil).CloseSend))
}

// Context mocks base method
func (m *MockCliToHub_ExecuteClient) Context() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context
func (mr *MockCliToHub_ExecuteClientMockRecorder) Context() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockCliToHub_ExecuteClient)(nil).Context))
}

// Header mocks base method
func (m *MockCliToHub_ExecuteClient) Header() (metadata.MD, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Header")
	ret0, _ := ret[0].(metadata.MD)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Header indicates an expected call of Header
func (mr *MockCliToHub_ExecuteClientMockRecorder) Header() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Header", reflect.TypeOf((*MockCliToHub_ExecuteClient)(nil).Header))
}

// Recv mocks base method
func (m *MockCliToHub_ExecuteClient) Recv() (*idl.Message, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*idl.Message)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv
func (mr *MockCliToHub_ExecuteClientMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockCliToHub_ExecuteClient)(nil).Recv))
}

// RecvMsg mocks base method
func (m *MockCliToHub_ExecuteClient) RecvMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecvMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg
func (mr *MockCliToHub_ExecuteClientMockRecorder) RecvMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockCliToHub_ExecuteClient)(nil).RecvMsg), arg0)
}

// SendMsg mocks base method
func (m *MockCliToHub_ExecuteClient) SendMsg(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg
func (mr *MockCliToHub_ExecuteClientMockRecorder) SendMsg(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockCliToHub_ExecuteClient)(nil).SendMsg), arg0)
}

// Trailer mocks base method
func (m *MockCliToHub_ExecuteClient) Trailer() metadata.MD {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Trailer")
	ret0, _ := ret[0].(metadata.MD)
	return ret0
}

// Trailer indicates an expected call of Trailer
func (mr *MockCliToHub_ExecuteClientMockRecorder) Trailer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Trailer", reflect.TypeOf((*MockCliToHub_ExecuteClient)(nil).Trailer))
}
