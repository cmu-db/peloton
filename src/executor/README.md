# EXECUTOR

## Overview
This directory contains the query plan executors for the execution engine and as well as the logical tile wrappers for moving data between plan nodes. Each executor implements two methods from the `AbstractExecutor` base class: 

1. `DInit`: This method initializes the executor to prepare for executing the plan.
2. `DExecute`: This method is responsible for processing data from either its children nodes or an access method (i.e., table, index) and returning a logical tile.

## Additional Notes

This is for the interpretted execution engine.
