// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"testing"

	"github.com/blang/semver/v4"
)

func TestAllowedVersions(t *testing.T) {
	cases := []struct {
		name          string
		versions      []string
		validator     semver.Range
		validatorName string
		expected      bool
	}{
		{
			"allowed source versions",
			[]string{
				"5.28.0",
				"5.28.1",
				"5.50.0",
				"6.10.0",
				"6.10.1",
				"6.50.1",
			},
			SourceVersionAllowed,
			"SourceVersionAllowed",
			true,
		}, {
			"disallowed source versions",
			[]string{
				"4.3.0",
				"5.0.0",
				"5.27.0",
				"6.0.0",
				"6.9.0",
				"7.0.0",
			},
			SourceVersionAllowed,
			"SourceVersionAllowed",
			false,
		}, {
			"allowed target versions",
			[]string{
				"6.10.0",
				"6.10.1",
				"6.50.1",
			},
			TargetVersionAllowed,
			"TargetVersionAllowed",
			true,
		}, {
			"disallowed target versions",
			[]string{
				"4.3.0",
				"5.0.0",
				"5.27.0",
				"5.28.0",
				"5.50.0",
				"6.0.0",
				"6.9.0",
				"7.0.0",
			},
			TargetVersionAllowed,
			"TargetVersionAllowed",
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, v := range c.versions {
				ver := semver.MustParse(v)
				actual := c.validator(ver)

				if actual != c.expected {
					t.Errorf("%s(%q) = %t, want %t", c.validatorName, v, actual, c.expected)
				}
			}
		})
	}
}
