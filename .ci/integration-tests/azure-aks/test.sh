#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/spark-operator.git
(cd spark-operator/ && ./scripts/run_tests.sh)
exit_code=$?
./operator-logs.sh spark > /target/spark-operator.log
exit $exit_code
