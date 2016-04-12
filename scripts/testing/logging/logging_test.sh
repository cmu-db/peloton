psql < create_table.sql
#psql < insert.sql &
#psql < insert.sql &
psql < insert.sql &
psql < insert.sql & 
sleep 5
psql < select.sql > before_crash.txt
sleep 15
# after checkpointing
pkill peloton
sleep 3
pg_ctl -D data start
sleep 3
# after recovery
psql < select.sql > after_crash.txt
diff before_crash.txt after_crash.txt

