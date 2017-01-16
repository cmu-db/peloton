#!/bin/sh
git stash -q --keep-index
python script/validators/source_validator.py
RESULT=$?
git stash pop -q
[ $RESULT -ne 0 ] && exit 1
exit 0
