#!/bin/bash

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 out.xml out.log" 1>&2
    exit 1
fi

if [ -z "${DH_PREFIX}" ]; then
    echo "$0: Environment variable DH_PREFIX is not set, aborting." 1>&2
    exit 1
fi

cd $DH_PREFIX/src/rdeephaven

OUT_XML="$1"
OUT_LOG="$2"
export DHCPP=/opt/deephaven
PACKAGE_PATH="$DH_PREFIX/src/rdeephaven/src"

echo "--- Showing Directory ---"
echo "$PACKAGE_PATH"
ls -l /opt/deephaven/

R --no-save --no-restore <<EOF >& "${OUT_LOG}"
install.packages("covr")
library('testthat')
library(covr)
install.packages("htmltools")
install.packages("DT")
report(file = file.path("/out/r-coverage.html"))
options(testthat.output_file = '${OUT_XML}')
status = tryCatch(
  {
     test_package('rdeephaven', reporter = 'junit')
     0
  },
  error=function(e) 1
)
print(paste0('status=', status))
quit(save='no', status=status)
EOF

exit 0
