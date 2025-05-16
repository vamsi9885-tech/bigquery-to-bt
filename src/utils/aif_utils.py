from src.utils.df_utils import read_hdfs_file_header, log_error, put_hdfs_file, chk_hdfs_file,remove_special_char
from pyspark.sql.functions import *
import os
from datetime import datetime


def validate_header(sc,col_names, file, delimiter, select_columns,special_char=None,file_encoding_frmt=None):
    return_code=0
    file_cols = read_hdfs_file_header(sc, file,file_encoding_frmt)
    if special_char:
        file_cols = remove_special_char(special_char, file_cols)
    file_cols = file_cols.replace('"', '').replace(' ', '_')
    file_cols=map(lambda x: x.strip(), file_cols.split(delimiter))
    for x, y in zip(col_names, file_cols):
        if select_columns and x.upper() == "FILE_NAME":
            break
        if x.upper() != y.upper():
            return_code=1
    return return_code

def csv_importer(input_feed):
    """
        Method: csv_importer
        Purpose: To import files from the source table
        Arguments: Command line arguments
                :param input feed: the nth row of input feed list
                :param timestamp_val: timestamp value to compare with source files
    """
    return_code = 1
    fail_list = []
    file_extension=input_feed.get('file_extension',".csv")
    try:
        if input_feed.get('local_control_dir', '') != '':
            control_file_list = [x for x in os.listdir(input_feed['local_control_dir']) if
                                 x.endswith(file_extension) and x.startswith(input_feed['control_file'])]
            for file in control_file_list:
                chk_stat = chk_hdfs_file(input_feed['control_loc'] + "/" + file)
                if chk_stat != 0:
                    ex_code = put_hdfs_file(input_feed['local_control_dir'] + "/" + file,
                                            input_feed['control_loc'])
                    if ex_code != 0: fail_list.append(file)
        if input_feed.get('is_prefix','N') == 'Y':
           file_list = [x for x in os.listdir(input_feed['local_dir']) if
                     x.endswith(file_extension) and x.endswith(input_feed['source_table'] + file_extension)]
        else:
            file_list = [x for x in os.listdir(input_feed['local_dir']) if
                     x.endswith(file_extension) and x.startswith(input_feed['source_table'])]
        for file in file_list:
            chk_stat = chk_hdfs_file(input_feed['source_db'] + "/" + file)
            if chk_stat != 0:
                ex_code = put_hdfs_file(input_feed['local_dir'] + "/" + file,
                                        input_feed['source_db'])
                if ex_code != 0: fail_list.append(file)
        if not fail_list: return_code = 0
        return return_code
    except Exception as e:
        log_error(e)
        return return_code

def get_df_schema(hc, df):
    df1 = df.dtypes
    return ','.join(map(lambda r: r[0] + '  string', df1))