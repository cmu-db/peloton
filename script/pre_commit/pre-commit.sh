#!/bin/sh

files=$(git diff --name-only HEAD --diff-filter=d | grep '\.\(cpp\|h\)$')
python script/validators/source_validator.py --files $files
exit $?
