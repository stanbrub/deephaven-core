#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

XML=${1};
DO_COVERAGE=${2};

if [[ "${DO_COVERAGE}" == "true" ]]; then
  mkdir -p /out/coverage/
  go test -vet=all -v ./... -coverprofile=c.out 2>&1 | go-junit-report -set-exit-code -iocopy -out "$XML";
  go tool cover -func=c.out > /out/coverage/go-coverage.tsv;
else 
  go test -vet=all -v ./... 2>&1 | go-junit-report -set-exit-code -iocopy -out "$XML";
fi
