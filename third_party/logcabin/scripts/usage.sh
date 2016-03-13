#!/bin/bash

# Useful for reviewing usage messages on all executables.

for f in $(find build/ -type f -executable | sort); do
	echo "================================================================================"
	$f --help
done
echo "================================================================================"
