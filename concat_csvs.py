""" Script to time loaded/concatenating CSV files from disk.

Each approach performs these steps:

  - Load multiple CSVs from disk as dataframes in a list.
  - Concatenate together into a single dataframe.
  - Save that dataframe to disk.
"""

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
import itertools
import os
import os.path
from pathlib import Path
import tempfile
import timeit
import uuid
import pandas as pd
import make_files
import config


#: Number of repetitions in the timeit() function.
ITERATION_COUNT = 10


def list_csvs(dpath):
    """ List all paths to CSV files in a folder.
    """
    all_items = os.listdir(dpath)
    return [dpath / i for i in all_items if i.lower().endswith('.csv')]


def task(fpath):
    """ Loads a single file.
    """
    df = pd.read_csv(fpath)
    df['fpath'] = fpath
    return df


def single_threaded(filepaths):
    """ Loads all CSV files in a folder.
    """
    dfs = []
    for fpath in filepaths:
        df = task(fpath)
        dfs.append(df)
    combined = pd.concat(dfs)
    return combined


def multi_threaded(filepaths):
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(task, filepaths))
    combined = pd.concat(dfs)
    return combined


def one_thread_per_file(filepaths):
    with ThreadPoolExecutor(max_workers=len(filepaths)) as executor:
        dfs = list(executor.map(task, filepaths))
    combined = pd.concat(dfs)
    return combined


def multi_process(filepaths):
    with ProcessPoolExecutor() as executor:
        dfs = list(executor.map(task, filepaths))
    combined = pd.concat(dfs)
    return combined


if __name__ == '__main__':
    root_dirs = [config.M2SSD, config.SSD, config.HDD]  # [config.HDD, config.SSD, config.M2SSD]
    functions = [single_threaded, multi_threaded, one_thread_per_file, multi_process]
    file_counts = [2, 10, 100]
    row_counts = [200, 2000, 20000, 40000]

    results = []

    # Loop over root directories separately, so we only create the test datasets
    # for each drive once.
    for root_dir in root_dirs:
        for row_count in row_counts:
            with tempfile.TemporaryDirectory(dir=root_dir) as tmpdir:
                tmppath = Path(tmpdir)

                # Create dataset for this root directory.
                os.makedirs(root_dir, exist_ok=True)
                make_files.multi_process(tmppath, max(file_counts), row_count)
                filepaths = list_csvs(tmppath)

                # Size of the first file in the list (KB)
                filesize = filepaths[0].stat().st_size / 1024.0

                for file_count, fn in itertools.product(file_counts, functions):
                    runtime = timeit.timeit(partial(fn, filepaths[:file_count]), number=ITERATION_COUNT)
                    print(f'Run time {runtime:6.2f} s for {fn.__name__} reading {file_count} files ({filesize:.2f} KB) from {tmppath}.')
                    results.append([fn.__name__, str(tmppath), file_count, row_count, filesize, runtime])

    df = pd.DataFrame(results, columns=['Function', 'Path', 'FileCount', 'RowCount', 'FileKB', 'AvgRunTime'])
    df['DriveType'] = 'HDD'
    df['DriveType'].mask(df['Path'].str.startswith('D:'), 'M.2. SSD', inplace=True)
    df['DriveType'].mask(df['Path'].str.startswith('G:'), 'SSD', inplace=True)
    df['IterCount'] = ITERATION_COUNT

    print(df)
    df.to_csv(f'.temp/concat_csvs_results_{uuid.uuid4()}.csv', index=False)
