#!/bin/bash

# Runs the pre-commit hooks with every supplied configuration. This makes it
# easier to test against multiple compilers, debug and release versions, etc.
# You can find a bunch of configurations in hookmatrix.sample/ in the top-level
# project directory.

TIMES=3
FAILS=0

if [ $# -eq 0 ]; then
  echo "Specify scons configuration files as arguments"
  exit 1
fi

touch Local.sc
mv Local.sc Local.sc.bck
trap "rm -f Local.sc; mv Local.sc.bck Local.sc" EXIT

for f in $*; do
  echo ">>> Running hooks with config $f $TIMES times..."
  rm -f Local.sc
  cp Local.sc.bck Local.sc
  cat $f >> Local.sc
  for i in $(seq $TIMES); do
    ./hooks/pre-commit
    status=$?
    if [ $status -ne 0 ]; then
        echo "Hooks with config $f failed: $status"
        FAILS=$(( $FAILS + 1 ))
    fi
  done
done
exit $FAILS
