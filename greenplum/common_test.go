// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import "testing"

// MustCreateCluster creates a utils.Cluster and calls t.Fatalf() if there is
// any error.
//
// TODO: Consolidate with the same function in common_test.go in the hub
// package. This is tricky due to cycle imports and other issues.
func MustCreateCluster(t *testing.T, segs []SegConfig) *Cluster {
	t.Helper()

	cluster, err := NewCluster(segs)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	return cluster
}
