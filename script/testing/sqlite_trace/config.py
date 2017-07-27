# parameters for connecting to postgresql
pg_database = "SQLite_trace_test"
pg_username = "jenkins"
pg_password = "jenkinsTest123"

# parameters for connecting to postgresql
peloton_path = "../../../build/bin/peloton"
peloton_port = 52726
peloton_username = ""
peloton_password = ""

# sqllite trace file path
trace_file_root = "sqllite/trace"

# peloton-test path
peloton_test_path = "peloton-test"

# output
output_dir = "out"
peloton_log_file = "peloton_log"

# trace files
traces = map(lambda x: x.strip(), open("trace_files").readlines())

# sql keyword filter
# ** in upper case **
kw_filter = map(lambda x: x.strip(), open("keyword_filter").readlines())
