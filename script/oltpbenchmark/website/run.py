import os
from xml.etree import ElementTree
from xml.etree.ElementTree import Element


OLTPBENCH_DIR = '/home/wanshenl/oltpbench/'
OLTPBENCH_CONFIG_DIR = os.path.join(OLTPBENCH_DIR, 'config')
SCRIPT_DIR = '/home/wanshenl/scripts'
PELOTON_CONFIG_DIR = '/home/wanshenl/scripts/oltpconfig/'
POSTGRES_CONFIG_DIR = '/home/wanshenl/scripts/oltpconfig_pg/'
PELOTON_BASE_DIR = '/home/wanshenl/peloton'

BENCHMARKS = [
    # 'auctionmark',
    # 'chbenchmark',
    # 'db2tpcc',
    # 'epinions',
    # 'jpab',
    # 'linkbench',
    # 'mstpcc',
    # 'mysmixed',
    # 'noop',
    # 'oratpcc',
    # 'pgmixed',
    # 'pgtpcc',
    # 'pgwiki',
    # 'pgycsb',
    # 'resourcestresser',
    # 'seats',
    # 'sibench',
    # 'smallbank',
    'tatp',
    'tpcc',
    # 'tpch',
    # 'twitter',
    # 'voter',
    # 'wikipedia',
    'ycsb'
]



def config_xml_file(host, port, benchmark,
                    transaction, scalefactor, terminals,
                    time, upload_code, upload_url,
                    config_in, config_out, username, password):
    config_path = os.path.join(config_in, 'sample_{}_config.xml'.format(benchmark))
    xml = ElementTree.parse(config_path)
    root = xml.getroot()
    root.find("dbtype").text = "postgres"
    root.find("driver").text = "org.postgresql.Driver"
    root.find("DBUrl").text = "jdbc:postgresql://{0}:{1}/{2}".format(host, port, benchmark)
    root.find("username").text = username
    root.find("password").text = password
    uploadCodeElem = Element("uploadCode")
    uploadCodeElem.text = upload_code.decode('utf-8')
    root.append(uploadCodeElem)
    uploadUrlElem = Element("uploadUrl")
    uploadUrlElem.text = upload_url.decode('utf-8')
    root.append(uploadUrlElem)
    if 'isolation' not in root:
        isoElem = Element("isolation")
        isoElem.text = str(transaction)
        root.append(isoElem)
    root.find("isolation").text = str(transaction)
    root.find("scalefactor").text = str(scalefactor)
    root.find("terminals").text = str(terminals)
    for work in root.find('works').findall('work'):
        if 'time' not in work:
            timeElem = Element("time")
            timeElem.text = str(time)
            work.append(timeElem)
        work.find('time').text = str(time)
        work.find('rate').text = "unlimited"
    outfile = config_out + benchmark + '_config.xml'
    xml.write(outfile, encoding='UTF-8')
    print("Wrote {}".format(outfile))


def run_commands(commands):
    for command in commands:
        x = command.split(' ')
        if x[0] == 'cd':
            os.chdir(x[1])
        else:
            os.system(command)


def peloton_build():
    peloton_path = PELOTON_BASE_DIR
    install_script_path = os.path.join(peloton_path, 'script', 'installation')
    build_path = os.path.join(peloton_path, 'build')
    bin_path = os.path.join(peloton_path, 'bin')

    commands = [
        'cd ' + peloton_path,
        'git pull',
        'cd ' + install_script_path,
        'bash packages.sh',
        'cd ' + peloton_path,
        'mkdir -p build',
        'cd ' + build_path,
        'cmake -DCMAKE_BUILD_TYPE=Release ..',
        'make -j4',
        'cd ' + install_script_path,
        'bash create_certificates.sh',
        'export PATH=' + bin_path + ":$PATH",
        'cd ' + PELOTON_BASE_DIR,
    ]

    run_commands(commands)


def peloton_init():
    peloton_binary = os.path.join(PELOTON_BASE_DIR, 'build', 'bin', 'peloton')
    script_dir = SCRIPT_DIR

    commands = [
        'cd ' + SCRIPT_DIR,
        'killall peloton',
        peloton_binary + ' -port 15721 &',
        'sleep 3',
        'psql -U postgres -h localhost -p 15721 default_database < init.sql',
    ]

    run_commands(commands)


def generate_configs():
    upload_url = 'https://oltpbench.cs.cmu.edu/new_result/'
    transaction_isolation = 'TRANSACTION_SERIALIZABLE'
    scalefactor = 1
    terminals = 1
    client_time = 600

    config_in = OLTPBENCH_CONFIG_DIR

    def generate(config_dict):
        cfg = config_dict

        for benchmark in BENCHMARKS:
            config_xml_file(cfg['host'], cfg['port'], benchmark,
                            transaction_isolation, scalefactor, terminals,
                            client_time, cfg['upload_code'], upload_url,
                            config_in, cfg['config_out'],
                            cfg['username'], cfg['password'])

    peloton = {
        'host': 'localhost',
        'port': '15721',
        'config_out': PELOTON_CONFIG_DIR,
        'username': '',
        'password': '',
        'upload_code': ''
    }

    postgres = {
        'host': 'localhost',
        'port': '5432',
        'config_out': POSTGRES_CONFIG_DIR,
        'username': '',
        'password': '',
        'upload_code': ''
    }

    generate(peloton)
    generate(postgres)


def oltpbench_run():

    def oltpbench_run_bench(benchmark_name, config_path):
        commands = [
            'cd ' + OLTPBENCH_DIR,
            ('./oltpbenchmark -b {} -c {}'
             ' --create=true --load=true --execute=true --upload=true'
             ' -s 5'.format(benchmark_name, config_path))
        ]

        run_commands(commands)

    for benchmark in BENCHMARKS:
        config = os.path.join(PELOTON_CONFIG_DIR, '{}_config.xml'.format(benchmark))
        peloton_init()
        oltpbench_run_bench(benchmark, config)

        config = os.path.join(POSTGRES_CONFIG_DIR, '{}_config.xml'.format(benchmark))
        oltpbench_run_bench(benchmark, config)


def daily_run():
    peloton_build()
    generate_configs()
    oltpbench_run()


if __name__ == "__main__":
    daily_run()
