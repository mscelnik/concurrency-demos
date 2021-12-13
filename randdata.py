""" Make random dataframes.
"""

from string import ascii_uppercase

LETTERS = list(ascii_uppercase)
MAX_COLUMNS = len(LETTERS)


def make_df(row_count, columns):
    import numpy as np
    import pandas as pd
    values = np.random.rand(row_count, len(columns)) * 100.0
    return pd.DataFrame(values, columns=columns)


def size_df(row_count, columns, fpath):
    import os
    df = make_df(row_count, columns)
    if fpath.lower().endswith('.xlsx'):
        df.to_excel(fpath, index=False)
    else:
        df.to_csv(fpath, index=False)
    return os.stat(fpath).st_size / 1024


def rows_for_size(size, column_count=MAX_COLUMNS, fmt='csv'):
    """ Determine number of rows required to make a file of at least `size` KBs.

    Args:
        size (float): Desired file size in KBs.
        column_count (int): Number of columns to add to file.
        fmt (str): Either `csv` or `excel`.  Default is CSV.

    Returns:
        Number of rows (int) required for file size.
    """
    import os
    import os.path
    import tempfile

    if fmt.lower() in ('excel', 'xl', 'xls', 'xlsx'):
        fname = 'test.xlsx'
        is_excel = True
    else:
        fname = 'test.csv'
        is_excel = False

    columns = LETTERS[:column_count]

    # Guestimate initial number of rows from the dimensions.
    nrows = int(25 * size / column_count)

    with tempfile.TemporaryDirectory() as dpath:
        fpath = os.path.join(dpath, fname)

        # Set the upper bound.
        current_size = 0.0
        while current_size < size:
            nrows *= 2
            print(nrows)
            df = make_df(nrows, columns)
            if is_excel:
                df.to_excel(fpath, index=False)
            else:
                df.to_csv(fpath, index=False)
            current_size = os.stat(fpath).st_size / 1024

        # Binary search to find row count.
        lowrows = nrows / 2
        highrows = nrows
        while (highrows - lowrows) > 1:
            nrows = int((highrows + lowrows) / 2)

            df = make_df(nrows, columns)
            if is_excel:
                df.to_excel(fpath, index=False)
            else:
                df.to_csv(fpath, index=False)
            current_size = os.stat(fpath).st_size / 1024

            if current_size < size:
                lowrows, highrows = nrows, highrows
            else:
                lowrows, highrows = lowrows, nrows

            print(current_size, nrows, lowrows, highrows)

    return highrows


if __name__ == '__main__':
    for sz in [100, 1024, 10 * 1024]:
        n = rows_for_size(sz)
        print(sz, n)
