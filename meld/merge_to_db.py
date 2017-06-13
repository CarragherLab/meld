import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm
from meld import colfuncs
from meld import utils


class Merger(object):

    """
    Directory containing the .csv from a CellProfiler run

    Methods
    -------
    create_db :
        creates an sqlite database in the results directory
        directory - string, top level directory containing cellprofiler output
    to_db :
        loads the csv files in the directory and writes them as tables to the
        sqlite database created by create_db
    to_db_agg :
        like to_db, but aggregates the data on a specified column
    """

    def __init__(self, directory):
        """
        Get full filepaths of all files in a directory, including sub-directories
        """
        file_paths = []
        for root, _, files in os.walk(directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)
            self.file_paths = file_paths
        self.db_handle = None
        self.engine = None


    # create sqlite database
    def create_db(self, location, db_name="results"):
        """
        creates database

        Parameters:
        -----------
        location : string
            Location where to create the database
        db_name : string
            What to call the database at location

        Note:
        ------
        If a database already exists with the same location and name then
        this will act on the existing database rather than overwriting
        """
        if not db_name.endswith(".sqlite"):
            db_name += ".sqlite"
        db_path = os.path.join([location, db_name])
        self.db_handle = "sqlite:///{}".format(db_path)
        self.engine = create_engine(self.db_handle)


    # write csv files to database
    def to_db(self, select="DATA", header=0, **kwargs):
        """
        Parameters
        -----------
        select : string
            the name of the .csv file, this will also be the database table
        header : int or list
            the number of header rows.
        **kwargs : additional arguments to pandas.read_csv
        """
        # filter files
        file_paths = [f for f in self.file_paths if f.endswith(select+".csv")]
        # check there are files matching select argument
        if len(file_paths) == 0:
            raise ValueError("No files found matching '{}'".format(select))
        for x in tqdm(file_paths):
            if header == 0 or header == [0]:
                # dont need to collapse headers
                tmp_file = pd.read_csv(x, header=header, chunksize=10000,
                                       iterator=True, **kwargs)
                all_file = pd.concat(tmp_file)
                all_file.to_sql(select, con=self.engine, index=False,
                                if_exists="append")
            else:
                # have to collapse columns, means reading into pandas
                tmp_file = pd.read_csv(x, header=header, chunksize=10000,
                                       iterator=True, **kwargs)
                all_file = pd.concat(tmp_file)
                # collapse column names if multi-indexed
                if isinstance(all_file.columns, pd.core.index.MultiIndex):
                    all_file.columns = colfuncs.collapse_cols(all_file)
                else:
                    TypeError("Multiple headers selected, yet dataframe is not \
                               multi-indexed")
                # write to database
                all_file.to_sql(select, con=self.engine, index=False,
                                if_exists="append")


    def to_db_agg(self, select="DATA", header=0, by="ImageNumber",
                  method="median", prefix=False, **kwargs):
        """
        Parameters
        -----------
        select : string
            the name of the .csv file, this will also be the prefix of the
            database table name.
        header : int or list
            the number of header rows.
        by : string
            the column by which to group the data by.
        method : string (default="median")
            method by which to average groups, median or mean
        prefix : Boolean
            whether the metadata label required for discerning featuredata
            and metadata needs to be a prefix, or can just be contained within
            the column name
        **kwargs : additional arguments to pandas.read_csv and aggregate
        """
        # filter files
        file_paths = [f for f in self.file_paths if f.endswith(select + ".csv")]
        # check there are files matching select argument
        if len(file_paths) == 0:
            raise ValueError("No files found matching '{}'".format(select))
        for x in tqdm(file_paths):
            if header == 0:
                tmp_file = pd.read_csv(x, header=header, **kwargs)
                tmp_agg = utils.aggregate(tmp_file, on=by, method=method,
                                          **kwargs)
                tmp_agg.to_sql(select + "_agg", con=self.engine, index=False,
                               if_exists="append")
            else:
                tmp_file = pd.read_csv(x, header=header, **kwargs)
                # collapse multi-indexed columns
                if isinstance(tmp_file.columns, pd.core.index.MultiIndex):
                    tmp_file.columns = colfuncs.collapse_cols(tmp_file)
                else:
                    raise TypeError("Multiple headers selected, yet dataframe is not\
                               multi-indexed")
                tmp_agg = utils.aggregate(tmp_file, on=by, method=method,
                                          **kwargs)
                tmp_agg.to_sql(select + "_agg", con=self.engine, index=False,
                               if_exists="append")


