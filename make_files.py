""" Script to create 10,000 random CSV files in a folder and time it.

Each file has 26 columns (A-Z) plus an index, and 200 rows of random floats in
the range 0-100.
"""

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
import itertools
import os
import os.path
from pathlib import Path
from string import ascii_uppercase
import tempfile
import timeit
import uuid
import numpy as np
import pandas as pd
from pandas.core.algorithms import diff
import config


FILE_COUNT = 10
ROW_COUNT = 200

#: Number of repetitions in the timeit() function.
ITERATION_COUNT = 10

columns = list(ascii_uppercase)
column_count = len(columns)


def make_df(row_count):
    values = np.random.rand(row_count, column_count) * 100.0
    return pd.DataFrame(values, columns=columns)


def task(i, dpath, df):
    # Choose a file path.
    fpath = dpath / f'data-{i:05d}.csv'
    df.to_csv(fpath)


def single_threaded(dpath, file_count=FILE_COUNT, row_count=ROW_COUNT):
    os.makedirs(dpath, exist_ok=True)
    df = make_df(row_count)
    for i in range(file_count):
        task(i, dpath, df)
    return df


def multi_threaded(dpath, file_count=FILE_COUNT, row_count=ROW_COUNT):
    os.makedirs(dpath, exist_ok=True)
    df = make_df(row_count)
    with ThreadPoolExecutor() as executor:
        for i in range(file_count):
            executor.submit(task, i, dpath, df)
    return df


def one_thread_per_file(dpath, file_count=FILE_COUNT, row_count=ROW_COUNT):
    os.makedirs(dpath, exist_ok=True)
    df = make_df(row_count)
    with ThreadPoolExecutor(max_workers=file_count) as executor:
        for i in range(file_count):
            executor.submit(task, i, dpath, df)
    return df


def multi_process(dpath, file_count=FILE_COUNT, row_count=ROW_COUNT):
    os.makedirs(dpath, exist_ok=True)
    df = make_df(row_count)
    with ProcessPoolExecutor() as executor:
        for i in range(file_count):
            executor.submit(task, i, dpath, df)
    return df


if __name__ == '__main__':
    root_dirs = [config.HDD, config.SSD, config.M2SSD]
    functions = [single_threaded, multi_threaded, one_thread_per_file, multi_process]
    file_counts = [1000, 100, 10, 2]
    row_counts = [10000, 2500, 250]

    results = []

    for file_count, row_count, root_dir, fn in itertools.product(file_counts, row_counts, root_dirs, functions):
        os.makedirs(root_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=root_dir) as tmpdir:
            tmppath = Path(tmpdir)
            runtime = timeit.timeit(partial(fn, tmppath, file_count, row_count), number=ITERATION_COUNT)

            print(f'Run time {runtime:6.2f} s for {fn.__name__} creating {file_count} files ({row_count} rows) in {tmppath}.')
            results.append([fn.__name__, str(tmppath), file_count, row_count, runtime])

    df = pd.DataFrame(results, columns=['Function', 'Path', 'FileCount', 'RowCount', 'RunTime'])
    df['DriveType'] = 'HDD'
    df['DriveType'].mask(df['Path'].str.startswith('D:'), 'M.2. SSD', inplace=True)
    df['DriveType'].mask(df['Path'].str.startswith('G:'), 'SSD', inplace=True)

    print(df)
    df.to_csv(f'.temp/make_files_results_{uuid.uuid4()}.csv', index=False)
