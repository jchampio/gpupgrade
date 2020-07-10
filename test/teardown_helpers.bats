#! /usr/bin/env bats
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

load helpers
load teardown_helpers

# Prints the number of arguments passed, and the arguments themselves, on
# separate lines.
print_args() {
    echo ${#@}
    for arg in "$@"; do
        echo "$arg"
    done
}

@test "run_teardowns runs all registered commands in reverse order" {
    register_teardown echo first
    register_teardown print_args one "two three" four
    register_teardown echo last
    
    run run_teardowns

    diff -U2 <(echo "$output") - <<'EOF' || fail "run_teardowns printed unexpected output (see diff)"
running teardown: echo last
last
running teardown: print_args one two\ three four
3
one
two three
four
running teardown: echo first
first
EOF
}

@test "run_teardowns clears the registered list" {
    local was_run=0
    register_teardown eval was_run=1

    run_teardowns
    (( was_run )) || fail "teardown was not run"

    was_run=0
    run_teardowns
    (( ! was_run )) || fail "teardown was re-run incorrectly"
}