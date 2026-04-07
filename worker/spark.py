import argparse
import csv
import os
import time

from pyspark import StorageLevel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count

RESULTS_FILE = '/workspace/results/metrics.csv'

STRING_COLS = ['track_genre', 'explicit']
NUMERIC_COLS = [
    'popularity',
    'duration_ms',
    'danceability',
    'energy',
    'loudness',
    'speechiness',
    'acousticness',
    'instrumentalness',
    'liveness',
    'valence',
    'tempo'
]
ALL_COLS = STRING_COLS + NUMERIC_COLS


def append_metrics(row):
    exists = os.path.exists(RESULTS_FILE)
    with open(RESULTS_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow([
                'experiment',
                'total_time_sec',
                'kmeans_time_sec',
                'groupby_time_sec',
                'rows_count'
            ])
        writer.writerow(row)


def prepare_dataset(spark, local_input, hdfs_input, hdfs_block_size_mb=None):
    df = spark.read.option('header', True).option('inferSchema', True).csv(f'file://{local_input}')
    writer = df.write.mode('overwrite')

    if hdfs_block_size_mb:
        block_size = hdfs_block_size_mb * 1024 * 1024
        spark.conf.set('spark.hadoop.dfs.blocksize', str(block_size))
        writer = writer.option('parquet.block.size', str(block_size))

    writer.parquet(hdfs_input)


def run_experiment(spark, mode, cluster, hdfs_input):
    total_start = time.perf_counter()

    df = spark.read.parquet(hdfs_input).select(*ALL_COLS)

    for c in STRING_COLS:
        df = df.withColumn(c, col(c).cast('string'))
    for c in NUMERIC_COLS:
        df = df.withColumn(c, col(c).cast('double'))

    df = df.dropna(subset=ALL_COLS)

    if mode == 'opt':
        parts = max(spark.sparkContext.defaultParallelism * 2, 4)
        spark.conf.set('spark.sql.adaptive.enabled', 'true')
        spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        spark.conf.set('spark.sql.shuffle.partitions', str(parts))
        df = df.repartition(parts)

    prepared = VectorAssembler(
        inputCols=[
            'danceability',
            'energy',
            'loudness',
            'speechiness',
            'acousticness',
            'instrumentalness',
            'liveness',
            'valence',
            'tempo',
            'popularity',
            'duration_ms'
        ],
        outputCol='features'
    ).transform(df)

    kmeans_start = time.perf_counter()

    clustered = KMeans(
        k=5,
        seed=42,
        featuresCol='features',
        predictionCol='cluster',
        maxIter=20
    ).fit(prepared).transform(prepared).select(
        'cluster',
        'track_genre',
        'explicit',
        'danceability',
        'energy',
        'tempo',
        'valence',
        'popularity'
    )

    if mode == 'opt':
        clustered = clustered.persist(StorageLevel.MEMORY_AND_DISK)
        clustered.count()

    kmeans_time = time.perf_counter() - kmeans_start

    groupby_start = time.perf_counter()

    cluster_counts = clustered.groupBy('cluster').count().collect()

    clustered.groupBy('cluster').agg(
        avg('danceability').alias('avg_danceability'),
        avg('energy').alias('avg_energy'),
        avg('tempo').alias('avg_tempo'),
        avg('valence').alias('avg_valence'),
        avg('popularity').alias('avg_popularity')
    ).collect()

    clustered.groupBy('cluster', 'track_genre').agg(count('*').alias('tracks_count')).collect()
    clustered.groupBy('cluster', 'explicit').agg(count('*').alias('tracks_count')).collect()

    groupby_time = time.perf_counter() - groupby_start
    total_time = time.perf_counter() - total_start
    rows_count = sum(row['count'] for row in cluster_counts)

    if mode == 'opt':
        clustered.unpersist()

    append_metrics([
        f'{cluster}_{mode}',
        round(total_time, 3),
        round(kmeans_time, 3),
        round(groupby_time, 3),
        rows_count
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['prepare', 'base', 'opt'], required=True)
    parser.add_argument('--cluster', required=True)
    parser.add_argument('--local-input')
    parser.add_argument('--hdfs-input', required=True)
    parser.add_argument('--hdfs-block-size-mb', type=int)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f'spotify-{args.cluster}-{args.mode}')
        .config('spark.ui.showConsoleProgress', 'false')
        .config('spark.sql.parquet.compression.codec', 'snappy')
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('ERROR')

    try:
        if args.mode == 'prepare':
            if not args.local_input:
                raise ValueError('--local-input required')
            prepare_dataset(spark, args.local_input, args.hdfs_input, args.hdfs_block_size_mb)
        else:
            run_experiment(spark, args.mode, args.cluster, args.hdfs_input)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
