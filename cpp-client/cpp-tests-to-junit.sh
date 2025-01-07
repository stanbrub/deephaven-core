#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 out.xml out.log" 1>&2
    exit 1
fi

if [ -z "${DH_PREFIX}" ]; then
    echo "$0: Environment variable DH_PREFIX is not set, aborting." 1>&2
    exit 1
fi

${DH_PREFIX}/bin/dhclient_tests --reporter XML --out "$1" 2>&1 | tee "$2"
apt-get -y update 2>&1 >> "$2"
apt-get -y install gcovr 2>&1 >> "$2"
GCOVR_OUT=/out/coverage.log
cd ${DH_PREFIX} 2>&1 > "${GCOVR_OUT}"
find / -name "*.gcno" 2>&1 >> "${GCOVR_OUT}"
find / -name "*.gcda" 2>&1 >> "${GCOVR_OUT}" | xargs -n 1 cat
find / -name "*.o" 2>&1 >> "${GCOVR_OUT}"
gcovr -r . . --csv /out/coverage.csv 2>&1 >> "${GCOVR_OUT}"
gcovr -r . . --html /out/coverage.html 2>&1 >> "${GCOVR_OUT}"
echo "GCOVR EXIT $?" 2>&1 >> "${GCOVR_OUT}"

