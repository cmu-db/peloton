## Terminal 1
``` sh
cd /home/pavlo/Documents/Peloton/Github/peloton
script -f /tmp/peloton.log
ssh dev4 "$(pwd)/build/bin/peloton --index_tuner"
```
## Terminal 2
``` sh
cd /home/pavlo/Documents/OLTPBenchmark/oltpbench
ssh ottertune "cd $(pwd) && ant build execute -Dbenchmark=tpcc -Dconfig=config/tpcc_config_peloton.xml -Dcreate=true -Dload=true -Dexecute=false"
```
## Terminal 3
``` sh
cd /home/pavlo/Documents/Peloton/Github/peloton/script/demo
ssh dev4 "cd $(pwd) && ./init-demo.sh"
ssh dev4 "cd $(pwd) && ./start-demo.sh"
./throughput-demo.py
```
