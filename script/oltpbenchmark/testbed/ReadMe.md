## Preparation 
* Git clone the oltpbench under this directory (peloton/script/oltpbenchmark/testbed)
* Launch peloton on another machine
* Write down the IP/hostname and port number of the running peloton instance in measure-performance-*.py's 'db_host' and 'db_port' 

## How to get tpcc/ycsb numbers?
* Properly set the number of thread you want to test in the for loop in the main function of measue-performance-*.py.
* Properly set the test_time in the same python file
* Complete the preparation
* Run one of these two:
~~~~
$ python measure-performance-tpcc.py
~~~~
~~~~
$ python measure-performance-ycsb.py
~~~~

* To get the throughput results, run
~~~~
$ ./gather_tpcc.sh
~~~~

~~~~
$ ./gather_ycsb.sh
~~~~


## How to get tpcc/ycsb perf data?
* Properly set the number of thread you want to test in the for loop in the main function of measue-performance-*.py.
* Set the test_time to a large value like 1800. 
* Launch peloton on other machine using the following command. Make sure that the timeout is far greater than the loading time. 
~~~~
$ sudo timeout 600s  perf record -a -g -s -- ./bin/peloton
~~~~
* Collect data by
~~~~
$ sudo perf report
~~~~

## How to get tpcc/ycsb callgrind data?
* Properly set the number of thread you want to test in the for loop in the main function of measue-performance-*.py.
* Set the test_time to a large value like 1800
* Launch peloton on other machine using the following command.
~~~~
valgrind --tool=callgrind --trace-children=yes --instr-atstart=no ./bin/peloton
~~~~
* Start a new terminal in the peloton machine, run the following command.
Note that this command sleep for a while to wait for loading. Then it attaches the valgrind to the peloton process. After collection enough data, it detaches valgrind.
Make sure the first sleep time is larger than the loading time and the second waiting time is long enough.
~~~~
sleep 300 && callgrind_control -i on && sleep 400 && callgrind_control -i off
~~~~
* Kill the peloton instance after the above command is finish, collect the callgrind data

## Notes
* By default the script automatically calls 'git pull' and recompile oltpbench before each run of the benchmark.
* TODO1: The scripts have problem getting result from the latest oltpbench because we move the result to the 'results' dir in and I hard code the result path.
* TODO2: The way 'gather_*.sh' report numbers may not be stable enough. Idealy it should average across every samples. But currently we just take the 6th sample and treat it as the average.
* TODO3: In the for loop of the python file, we should kill the remote peloton instance and start a new one for each run. 

