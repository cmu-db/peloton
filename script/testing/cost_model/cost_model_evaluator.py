import argparse
import psycopg2
import re
import socket
import subprocess
import time

from collections import namedtuple

# TODO add more stats
ExecutionStats = namedtuple('ExecutionStats', ['average'])


class Peloton(object):
    def __init__(self, peloton_path, port, cost_calculator=None):
        self.peloton_path = peloton_path
        self.peloton_port = port
        self.cost_calculator = cost_calculator
        self.peloton_process = None
        self.peloton_output_fd = None
        self.conn = None

    def run(self):
        outfile = "/tmp/peloton_log.txt"
        args = [self.peloton_path, "-port", str(self.peloton_port)]
        if self.cost_calculator:
            args += ["-cost_calculator", self.cost_calculator]
        args += ['-codegen', 'false']
        args += ['-hash_join_bloom_filter', 'false']
        self.peloton_output_fd = open(outfile, "w+")
        self.peloton_process = subprocess.Popen(args, stdout=self.peloton_output_fd, stderr=self.peloton_output_fd)
        self.wait()
        self.conn = psycopg2.connect(
            "dbname=default_database user=postgres password=postgres host=localhost port={}".format(self.peloton_port))

    def stop(self):
        if not self.peloton_process:
            raise Exception("No peloton process to stop")
        self.peloton_process.poll()
        if self.peloton_process.returncode is not None:
            # Peloton terminated already
            self.peloton_output_fd.close()
            msg = "Peloton terminated with return code {}".format(self.peloton_process.returncode)
            raise RuntimeError(msg)

        # still(correctly) running, terminate it
        self.conn.close()
        self.peloton_process.terminate()
        return

    def wait(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # max wait of 10s in 0.1s increments
        for i in xrange(100):
            try:
                s.connect(('localhost', self.peloton_port))
                s.close()
                print("connected to server in {} seconds".format(i * 0.1))
                return
            except:
                time.sleep(0.1)
                continue
        return

    def process_query(self, query):
        cur = self.conn.cursor()
        rows = None
        if query:
            cur.execute(query)
            try:
                rows = cur.fetchall()
            except Exception:
                pass
        self.conn.commit()
        cur.close()
        return rows


def get_query_list(sql_string_file):
    with open(sql_string_file, 'r') as infile:
        sql_string = infile.read()
    sql_string = sql_string.replace('\n', '')
    sql_string = re.sub(' +', ' ', sql_string)
    query_list = [x for x in sql_string.split(';') if x]
    return query_list


def execute_sql_statements(data_path, peloton):
    query_list = get_query_list(data_path)
    for query in query_list:
        peloton.process_query(query)


def execute_sql_statements_with_stats(data_path, peloton, execution_count):
    execution_stat_list = []
    explain_result_list = []
    results = []
    query_list = get_query_list(data_path)
    for i, query in enumerate(query_list):
        sum_time = 0
        for _ in xrange(execution_count):
            start_time = time.time()
            rows = peloton.process_query(query)
            end_time = time.time()
            sum_time += (end_time - start_time)
        result = {
            'num': i + 1,
            'query': query,
            'num_rows': len(rows) if rows else 0,
            'execution_stats': ExecutionStats(average=sum_time / execution_count),
            'explain_result': peloton.process_query("".join(["EXPLAIN ", query]))
        }
        results.append(result)
    return results


def analyze(peloton):
    # Note this doesn't actually do anything. When https://github.com/cmu-db/peloton/issues/1360 is resolved, this will work
    peloton.process_query("ANALYZE;")


def run_pipeline(args):
    peloton = Peloton(args.peloton_path, args.port, args.cost_model)
    try:
        peloton = Peloton(args.peloton_path, args.port, args.cost_model)
        peloton.run()
        execute_sql_statements(args.data_load_path, peloton)
        analyze(peloton)
        results = execute_sql_statements_with_stats(args.data_query_path, peloton, args.query_count)
        for result in results:
            print 'Query Num: {}'.format(result['num'])
            print 'Query: {}'.format(result['query'])
            print 'Num Result Rows: {}'.format(result['num_rows'])
            print result['execution_stats']
            for row in result['explain_result']:
                print row
        peloton.stop()
    except Exception as e:
        print e
        peloton.stop()


def main():
    parser = argparse.ArgumentParser(description="Evaluate the provided cost model")
    parser.add_argument("--cost-model")
    parser.add_argument("--port", default=15721, help="Optional port override if you aren't using 15721")
    parser.add_argument("--peloton-path", default="peloton",
                        help="Optional path to peloton binary if peloton is not on your path")
    parser.add_argument('--query-count', default=500,
                        help="Number of times to run the query defined in the data query path")
    # For now, data load path needs to include analyze statements at end. we don't support analyze on all tables
    parser.add_argument("data_load_path")
    parser.add_argument("data_query_path")
    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
