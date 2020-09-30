// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package cli

import "github.com/blang/semver/v4"

var (
	// SourceVersionAllowed returns whether or not the given semver.Version is a
	// valid source GPDB cluster version.
	SourceVersionAllowed semver.Range

	// TargetVersionAllowed returns whether or not the given semver.Version is a
	// valid target GPDB cluster version.
	TargetVersionAllowed semver.Range
)

// Source and Target Ranges: modify these lists to control what will be allowed
// by the utility.

var sourceRanges []semver.Range = []semver.Range{
	semver.MustParseRange(">=5.28.0 <6.0.0"), // acceptable 5X releases
	semver.MustParseRange(">=6.10.0 <7.0.0"), // acceptable 6X releases
}

var targetRanges []semver.Range = []semver.Range{
	semver.MustParseRange(">=6.10.0 <7.0.0"), // acceptable 6X releases
}

// The below boilerplate turns the source/targetRanges variables into
// Source/TargetVersionAllowed. You shouldn't need to touch it.

func init() {
	accumulateRanges(&SourceVersionAllowed, sourceRanges)
	accumulateRanges(&TargetVersionAllowed, targetRanges)
}

func accumulateRanges(a *semver.Range, ranges []semver.Range) {
	for _, r := range ranges {
		if *a == nil {
			*a = r
		} else {
			*a = a.OR(r)
		}
	}
}
