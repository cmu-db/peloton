#!/usr/bin/env python
import argparse
import os
import shutil
import socket
import subprocess
import time
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

class BaseXMLConfig:
    """Contains XML configuration."""
    host = ''           # db host
    port = ''           # db port
    dbtype = ''         # db type
    driver = ''         # db driver
    jdbc_prefix = ''    # jdbc_prefix
    username = ''       # db username
    password = ''       # db password
    benchmark = ''      # benchmark name
    isolation = ''      # transaction isolation level
    scalefactor = ''    # scale factor for benchmark
    terminals = ''      # number of terminals
    time = ''           # time for work
    rate = ''           # rate for work
    uploadCode = ''     # OLTPBenchmark project upload code
    uploadUrl = ''      # OLTPBenchmark project upload URL
    config_in = ''      # directory with sample_BENCHMARK_config.xml files
    config_out = ''     # directory to place generated config files

class PostgresConfig(BaseXMLConfig):
    host = 'localhost'
    port = '5432'
    dbtype = 'postgres'
    driver = 'org.postgresql.Driver'
    jdbc_prefix = 'jdbc:postgresql'

class PelotonConfig(BaseXMLConfig):
    host = 'localhost'
    port = '15721'
    dbtype = 'postgres'
    driver = 'org.postgresql.Driver'
    jdbc_prefix = 'jdbc:postgresql'

class PelotonBuilder:
    """Builds the latest version of Peloton."""

    def __init__(self, peloton_path):
        self.peloton_path = peloton_path
        self.pkg_path = os.path.join(peloton_path, 'script', 'installation')
        self.build_path = os.path.join(peloton_path, 'build')
        self.bin_path = os.path.join(peloton_path, 'bin')

    def _git_pull(self):
        os.chdir(self.peloton_path)
        subprocess.check_call(['git', 'pull'])

    def _install_packages(self):
        os.chdir(self.pkg_path)
        subprocess.check_call(['bash', 'packages.sh'])

    def _run_cmake(self):
        shutil.rmtree(self.build_path)
        os.makedirs(self.build_path)
        os.chdir(self.build_path)
        subprocess.check_call(['cmake', '-DCMAKE_BUILD_TYPE=Release', '..'])
        subprocess.check_call(['make', '-j4'])

    def _enable_ssl(self):
        os.chdir(self.pkg_path)
        subprocess.check_call(['bash', 'create_certificates.sh'])

    def _add_path(self):
        os.environ['PATH'] += os.pathsep + self.bin_path

    def build(self):
        """Pull and build the latest Peloton version."""
        self._git_pull()
        self._install_packages()
        self._run_cmake()
        self._enable_ssl()
        self._add_path()

class PelotonRunner:

    def __init__(self, peloton_path, peloton_host, peloton_port, peloton_username, benchmarks):
        self.peloton_path = peloton_path
        self.peloton_host = peloton_host
        self.peloton_port = peloton_port
        self.peloton_username = peloton_username
        self.benchmarks = benchmarks
        self.running = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.running:
            self.stop()
            self.running = False

    def _run_peloton(self):
        """Start the Peloton server."""
        peloton_binary = os.path.join(self.peloton_path, 'build', 'bin', 'peloton')
        self.peloton_process = subprocess.Popen([peloton_binary, '-port', str(self.peloton_port)])
        self._wait_for_peloton()

    def _wait_for_peloton(self):
        """ Wait for the peloton server to come up."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # max wait of 10s in 0.1s increments
        for i in range(100):
            try:
                s.connect((self.peloton_host, self.peloton_port))
                s.close()
                print("Connected to server in {} seconds".format(i*0.1))
                self.running = True
                return
            except:
                time.sleep(0.1)
                continue

    def _stop_peloton(self):
        """Stop the Peloton server."""
        self.peloton_process.poll()
        if self.peloton_process.returncode is not None:
            # Peloton terminated already
            msg = "Peloton terminated with return code {}".format(
                self.peloton_process.returncode)
            raise RuntimeError(msg)
        # still (correctly) running, terminate it
        self.peloton_process.terminate()
        self.peloton_process.wait()
        self.running = False

    def _init_sql(self):
        for benchmark in self.benchmarks:
            create_commands = []
            create_commands.append('-c')
            create_commands.append('CREATE DATABASE {};'.format(benchmark))

            psql_command = ['psql', '-U', self.peloton_username,
                            '-h', self.peloton_host,
                            '-p', str(self.peloton_port),
                            'default_database'] + create_commands

            subprocess.check_call(psql_command)

    def init(self):
        """Starts the Peloton server and creates necessary tables."""
        self._run_peloton()
        self._init_sql()

    def stop(self):
        """Stops the Peloton server."""
        self._stop_peloton()

class ConfigGenerator:
    """Generates configuration files."""

    def _config_xml_file(self, cfg):
        """
        Creates the XML file for a given configuration.

        cfg : derived from BaseXMLConfig
        """
        cfg_name = 'sample_{}_config.xml'.format(cfg.benchmark)
        cfg_path = os.path.join(cfg.config_in, cfg_name)
        xml = ElementTree.parse(cfg_path)
        root = xml.getroot()

        options = [
            'dbtype',
            'driver',
            'DBUrl',
            'username',
            'password',
            'isolation',
            'scalefactor',
            'terminals',
            'uploadCode',
            'uploadUrl',
            ]

        cfg.DBUrl = '{}://{}:{}/{}'.format(cfg.jdbc_prefix, cfg.host, cfg.port, cfg.benchmark)

        # create any missing elements
        for opt in options:
            if root.find(opt) is None:
                elem = Element(opt)
                root.append(elem)

        # replace with config-specific options
        for opt in options:
            root.find(opt).text = str(getattr(cfg, opt))

        # replace time for all work
        for work in root.find('works').findall('work'):
            if work.find('time') is None:
                timeElem = Element('time')
                work.append(timeElem)
            work.find('time').text = str(cfg.time)
            work.find('rate').text = str(cfg.rate)

        outfile = os.path.join(cfg.config_out, '{}_config.xml'.format(cfg.benchmark))
        xml.write(outfile, encoding='UTF-8')
        print("Wrote {}".format(outfile))

    def generate_configs(self, benchmarks, cfg):
        """Generates all the configuration-specific benchmark files."""
        for benchmark in benchmarks:
            cfg.benchmark = benchmark
            self._config_xml_file(cfg)

class OLTPBenchRunner:
    """Runs OLTPBenchmark."""

    def __init__(self, oltpbench_dir, config_dir):
        self.oltpbench_dir = oltpbench_dir
        self.config_dir = config_dir

    def run(self, benchmark):
        cfg_path = os.path.join(self.config_dir, '{}_config.xml'.format(benchmark))
        os.chdir(self.oltpbench_dir)
        ret_code = subprocess.call([
            './oltpbenchmark',
            '-b', benchmark,
            '-c', cfg_path,
            '--create=true',
            '--load=true',
            '--execute=true',
            '--upload=true',
            '-s', '5'])
        print('OLTPBenchmark exit-code: {}'.format(ret_code))

if __name__ == '__main__':
    aparser = argparse.ArgumentParser(
        description='Generate configuration files, build and run Peloton, upload results.')
    aparser.add_argument('-oltpbench-dir', type=str,
                         help='Path to OLTPBenchmark', required=True)
    aparser.add_argument('-config-in', type=str,
                         help='Directory with sample_BENCHMARK_config.xml files', required=True)
    aparser.add_argument('-benchmarks', nargs='+',
                         help='Benchmarks to run', required=True)

    aparser.add_argument('-peloton-path', type=str,
                         help='Path to Peloton folder', required=True)
    aparser.add_argument('-peloton-username', type=str,
                         help='Peloton Username', required=True)
    aparser.add_argument('-peloton-upload-code', type=str,
                         help='Peloton Upload Code', required=True)
    aparser.add_argument('-peloton-config-out', type=str,
                         help='Output directory for Peloton-specific config', required=True)
    aparser.add_argument('--peloton-password', type=str,
                         help='Peloton Password', default='')
    aparser.add_argument('--peloton-host', type=str,
                         help='Peloton Host (default: localhost)', default='localhost')
    aparser.add_argument('--peloton-port', type=int,
                         help='Peloton Port (default: 15721)', default=15721)
    aparser.add_argument('--peloton-upload-url', type=str,
                         default='https://oltpbench.cs.cmu.edu/new_result/',
                         help='Upload Url (default: https://oltpbench.cs.cmu.edu/new_result/)')

    aparser.add_argument('-postgres-username', type=str,
                         help='Postgres Username', required=True)
    aparser.add_argument('-postgres-upload-code', type=str,
                         help='Postgres Upload Code', required=True)
    aparser.add_argument('-postgres-config-out', type=str,
                         help='Output directory for Postgres-specific config', required=True)
    aparser.add_argument('--postgres-password', type=str,
                         help='Postgres Password', default='')
    aparser.add_argument('--postgres-host', type=str,
                         help='Postgres Host (default: localhost)', default='localhost')
    aparser.add_argument('--postgres-port', type=int,
                         help='Postgres Port (default: 5432)', default=5432)
    aparser.add_argument('--postgres-upload-url', type=str,
                         default='https://oltpbench.cs.cmu.edu/new_result/',
                         help='Upload Url (default: https://oltpbench.cs.cmu.edu/new_result/)')

    aparser.add_argument('--scalefactor', type=int, default=1,
                         help='The scale factor. (default: 1)')
    aparser.add_argument('--isolation', type=str, default='TRANSACTION_SERIALIZABLE',
                         help='The transaction isolation level (default: TRANSACTION_SERIALIZABLE')
    aparser.add_argument('--time', type=int, default=600,
                         help='How long to execute each benchmark trial (default: 600)')
    aparser.add_argument('--rate', type=str, default='unlimited',
                         help='Benchmark trial rate (default: unlimited)')
    aparser.add_argument('--terminals', type=int, default=1,
                         help='Number of terminals in each benchmark trial (default: 1)')
    args = vars(aparser.parse_args())

    peloton = PelotonConfig()
    peloton.host = args['peloton_host']
    peloton.port = args['peloton_port']
    peloton.username = args['peloton_username']
    peloton.password = args['peloton_password']
    peloton.config_out = args['peloton_config_out']
    peloton.uploadCode = args['peloton_upload_code']
    peloton.uploadUrl = args['peloton_upload_url']

    postgres = PostgresConfig()
    postgres.host = args['postgres_host']
    postgres.port = args['postgres_port']
    postgres.username = args['postgres_username']
    postgres.password = args['postgres_password']
    postgres.config_out = args['postgres_config_out']
    postgres.uploadCode = args['postgres_upload_code']
    postgres.uploadUrl = args['postgres_upload_url']

    def set_all(attrib, value):
        setattr(peloton, attrib, value)
        setattr(postgres, attrib, value)

    for arg in ['config_in', 'isolation', 'scalefactor', 'terminals', 'time', 'rate']:
        set_all(arg, args[arg])

    # main logic
    benchmarks = args['benchmarks']
    oltpbench_dir = args['oltpbench_dir']

    # peloton
    peloton_path = args['peloton_path']
    ConfigGenerator().generate_configs(benchmarks, peloton)
    PelotonBuilder(peloton_path).build()
    with PelotonRunner(peloton_path, peloton.host, peloton.port, peloton.username,
                       benchmarks) as runner:
        oltp = OLTPBenchRunner(oltpbench_dir, peloton.config_out)
        for benchmark in benchmarks:
            runner.init()
            oltp.run(benchmark)
            runner.stop()

    # postgres
    ConfigGenerator().generate_configs(benchmarks, postgres)
    oltp = OLTPBenchRunner(oltpbench_dir, postgres.config_out)
    for benchmark in benchmarks:
        oltp.run(benchmark)
