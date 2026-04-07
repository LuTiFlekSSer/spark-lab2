import os
import socket
import subprocess
import time

CLUSTER_TAG = os.environ['CLUSTER_TAG']
HDFS_URI = os.environ['HDFS_URI']
SPARK_MASTER = os.environ['SPARK_MASTER']
DATASET_PATH = os.environ['DATASET_PATH']

NAMENODE_HOST = os.environ['NAMENODE_HOST']
NAMENODE_PORT = int(os.environ['NAMENODE_PORT'])
SPARK_HOST = os.environ['SPARK_HOST']
SPARK_PORT = int(os.environ['SPARK_PORT'])

HDFS_BLOCK_SIZE_MB = os.environ.get('HDFS_BLOCK_SIZE_MB')

SPARK_ARGS = [
    'spark-submit',
    '--master', SPARK_MASTER,
    '--conf', 'spark.ui.showConsoleProgress=false',
    '--conf', 'spark.driver.memory=1g',
    '--conf', 'spark.executor.memory=1g',
    '--conf', 'spark.executor.memoryOverhead=512m'
]


def wait_for(host, port, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=3):
                return
        except OSError:
            time.sleep(5)
    raise RuntimeError(f'Timeout waiting for {host}:{port}')


def run(cmd):
    if subprocess.run(cmd).returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def main():
    os.makedirs('/workspace/results', exist_ok=True)

    wait_for(NAMENODE_HOST, NAMENODE_PORT)
    wait_for(SPARK_HOST, SPARK_PORT)

    hdfs_input = f'{HDFS_URI}/input/spotify_parquet_{int(time.time())}'

    prepare_cmd = SPARK_ARGS + [
        '/workspace/spark.py',
        '--mode', 'prepare',
        '--cluster', CLUSTER_TAG,
        '--local-input', DATASET_PATH,
        '--hdfs-input', hdfs_input
    ]

    if HDFS_BLOCK_SIZE_MB:
        prepare_cmd += ['--hdfs-block-size-mb', HDFS_BLOCK_SIZE_MB]

    print('[RUN]', prepare_cmd)
    run(prepare_cmd)

    print(
        '[RUN]', SPARK_ARGS + [
            '/workspace/spark.py',
            '--mode', 'base',
            '--cluster', CLUSTER_TAG,
            '--hdfs-input', hdfs_input
        ]
    )

    run(
        SPARK_ARGS + [
            '/workspace/spark.py',
            '--mode', 'base',
            '--cluster', CLUSTER_TAG,
            '--hdfs-input', hdfs_input
        ]
    )

    time.sleep(10)

    print(
        '[RUN]', SPARK_ARGS + [
            '/workspace/spark.py',
            '--mode', 'opt',
            '--cluster', CLUSTER_TAG,
            '--hdfs-input', hdfs_input
        ]
    )

    run(
        SPARK_ARGS + [
            '/workspace/spark.py',
            '--mode', 'opt',
            '--cluster', CLUSTER_TAG,
            '--hdfs-input', hdfs_input
        ]
    )

    print('[RUN] PLOT')
    run(['python3', '/workspace/plot_results.py'])


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('[ERROR]', e)
