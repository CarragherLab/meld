"""
Functions for dealing with dataframe column names
"""

import pandas as pd

def inflate_cols(dataframe, sep=" "):
    """
    Given a DataFrame with collapsed multi-index columns this will
    return a pandas DataFrame index. that can be used like so:
        df.columns = inflate_columns(df)
    """
    header_tuples = zip(*[col.split(sep) for col in dataframe.columns])
    return pd.MultiIndex.from_tuples(list(header_tuples))


def collapse_cols(dataframe, sep="_"):
    """Given a dataframe, will collapse multi-indexed columns names"""
    return [sep.join(col).strip() for col in dataframe.columns.values]
