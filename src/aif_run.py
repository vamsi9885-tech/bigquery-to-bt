import sys
sys.path.append("src.zip")
from src.run.create_spark_session import *
from src.utils.df_utils import *
from src.utils.aif_utils import *
from src.aif.aif_db_stage import *
from src.aif.aif_txt_file_stage import *
from src.aif.aif_stage_lake import *
from src.aif.aif_xml_land_stage_lake import *
from src.aif.aif_meta import *
from datetime import datetime, date
import getpass
import argparse, yaml, csv, string, json
#!/bin/python
#####################################################
#File Name: aif_run.py
#Type: Pyspark
#Purpose: To ingest data from source to Hadoop - Allianz Ingestion Framework - (AIF)
#Created: 06/07/2018
#Last Updated: 25/10/2019
#Author: Infosys
#####################################################

if __name__ == "__main__":
    """
        Method: Main
        Arguments: Command line arguments 
                :param env : "dev" "QA" "uat" "prod"
                :param input feed: csv file that contains table of files to be loaded to hadoop - conf/<input_feed.csv>
                :param conf_file: project specific configuration files - conf/wc_connection.yaml
                :param columns: used for file ingestion - header  and data type details are added to dictionary
                :param mode: "yarn", "client"  
    """
    parser = argparse.ArgumentParser()
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('--env', help='Provide environment [DEV, QA, PRD, UAT]', required=True)
    requiredNamed.add_argument('--input_feed', help='Please provide input feed csv file [/user/xyz/abc.csv]',
                               required=True)
    requiredNamed.add_argument('--conf_file',
                               help='Please provide connection configuration yaml file [/user/xyz/abc.yml]',
                               required=True)
    parser.add_argument('--columns',
                        help='Please table schema if not provided [/user/xyz/abc.csv]')
    parser.add_argument('--active_dt',
                        help='Please provide active date [2020-04-28]')
    parser.add_argument("--mode")
    parser.add_argument("--file_nm_ptrn")
    parser.add_argument('--tables', help='Provide list of tables to be executed')
    args = parser.parse_args()
    env = args.env.upper()
    active_dt = args.active_dt
    tables_list = None if (args.tables is None or args.tables.lower() == 'all') else args.tables.lower().split(",")
    properties = {}
    table_columns = {}
    # Read project specific connection.yaml file - eg: wc_connection.yaml
    with open(args.conf_file, 'r') as con:
        try:
            properties = yaml.load(con)
            log_info("Read wc_connections file !")
        except yaml.YAMLError as exc:
            log_error(exc)
            sys.exit(1)

    try:
        s = create_spark_session("AIF", "local[4]" if args.mode is None else args.mode)
    except Exception as e:
        log_info(e)
        log_error(e)
        sys.exit(1)
    (sc, hc) = s.get_sc_hc()
    APP_ID = sc.applicationId
    os.environ["HADOOP_CONF_DIR"] = '/etc/hadoop/conf/'
    os.environ["HADOOP_ROOT_LOGGER"] = 'ERROR'
    # Read the input_feed (csv) to ingest the data from RDBMS to Hadoop platform
    input_feed_list = []
    with open(args.input_feed, 'r') as f:
        records = csv.reader(f)
        headers = next(records)
        for row in records:
            if len(row) > 0 and isinstance(row, list):
                input_feed_list.append(OrderedDict(zip(headers, row)))
    # path for audit file creation
    meta_file = properties[env]["meta_path"] + "/" + time_stamped(properties[env][
                                                                      'lob'] + '_ingestion_metadata_') + '.csv'
    meta_wb = write_to_hdfs(meta_file)
    '''THis csv file will have input feed copied for the tables that were not loaded (failed to load) which can be 
        used to ingest again through AIF'''
    not_loaded_file_path = properties[env][
                               'not_loaded_path'] + "/" + APP_ID + time_stamped('notloaded_tables_feed_') + '.csv'
    not_loaded_wb = write_to_hdfs(not_loaded_file_path)
    not_loaded_wb.stdin.write(",".join(input_feed_list[0].keys()) + '\n')
    if len(input_feed_list) == 0:
        log_error("Input feed does not has value")
        sys.exit(1)

    # Loop through each entry in input feed to ingest data into hive table
    for input_feed in input_feed_list:
        if tables_list is None or input_feed['target_table'].lower() in tables_list:
            if input_feed['flow_type'] == "fs_hv":
                ''' File ingestion to hadoop. Currently csv files only supported'''
                if input_feed.get('hub_db', 0):
                    target_db = input_feed['hub_db'] if input_feed['hub_db'] != '' else input_feed['lake_db']
                elif input_feed.get('lake_db', 0):
                    target_db = input_feed['lake_db']
                else:
                    target_db = input_feed['target_db']
                aif_meta = AIFMeta(input_feed['flow_type'], input_feed['lob'], env, input_feed['load_type'],
                                   getpass.getuser(),
                                   input_feed['source_db'] + "/" + input_feed['source_table'],
                                   target_db + '.' + input_feed[
                                       'target_table'], input_feed['storage_type'], input_feed['split_column'],
                                   input_feed['lake_partitions'],
                                   string.replace(input_feed['bucket_columns'], ",", "|"),
                                   APP_ID)
                aif_meta.start_time = date_time_now()
                log_info("Args %s %s" % (args, args.columns))
                if args.columns is None:
                    table_columns = {}
                else:
                    table_columns = json.load(open(args.columns, 'r'))
                    table_columns = lower_dict(table_columns)

                log_info(table_columns)

                file_format = input_feed['file_format'].lower()
                src_table_name = input_feed['source_table'].lower()
                table_schema = table_columns[src_table_name] if src_table_name in table_columns.keys() else {}
                log_info("table_schema %s" % table_schema)
                col_list = ",".join(table_schema)
                flag = input_feed.get("extract_time")
                if flag is not None and flag.lower() == "true":
                    col_list = col_list + ",load_dt string,event_file_date string"
                else:
                    col_list = col_list + ",load_dt string,file_name string"
                log_info("col_list %s" % col_list)
                if input_feed['file_format'].lower() == "csv" and input_feed.get('load_data', 'Y') != 'N':
                    ret_code = db_stage(input_feed, hc, sc, properties, env, aif_meta, col_list)
                    if ret_code != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'Reference stage Fail'
                        log_error('hdfs to stage is Fail for table {db}.{tbl}'.format(db=input_feed['target_db'],
                                                                                      tbl=input_feed['target_table']))
                    else:
                        ret_stage_lake = stage_lake(input_feed, hc, properties, env, aif_meta, col_list)
                        if ret_stage_lake != 0:
                            not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                            status = 'Reference lake Fail'
                            log_error('stage to lake is Fail for table {db}.{tbl}'.format(db=input_feed['lake_db'],
                                                                                          tbl=input_feed[
                                                                                              'target_table']))
                    if ret_code == 0 and ret_stage_lake == 0:
                        status = 'Load Success'
                    aif_meta_write(meta_wb, aif_meta, status)
                else:
                    aif_meta_write(meta_wb, aif_meta, 'Skipped Ingestion')
                log_info("aif_meta.status  : %s" % aif_meta.status)

            elif input_feed['flow_type'] == "txt_hv":
                ''' File ingestion to hadoop hive database. 
                All kind of text file such as csv, txt, any text file with a delimiter'''
                target_db = input_feed['target_db']  # Must have a target_db
                source_db = input_feed["source_db"]
                patterns = [r'\W+']
                for p in patterns:
                    match = re.findall(p, source_db)
                if len("".join(set(match))) != 1:
                    log_error('source db {db} is not correct'.format(db=source_db))
                    sys.exit(1)
                source_db = source_db if source_db.startswith("/") else "/" + source_db
                source_db = source_db[0:-1] if source_db.endswith("/") else source_db
                aif_meta = AIFMeta(input_feed['flow_type'], input_feed['lob'], env, input_feed['load_type'],
                                   getpass.getuser(),
                                   source_db + "/" + input_feed['source_table'], target_db + '.' + input_feed[
                                       'target_table'], input_feed['storage_type'], input_feed['split_column'],
                                   input_feed['lake_partitions'],
                                   string.replace(input_feed['bucket_columns'], ",", "|"),
                                   APP_ID)
                # Starting time
                aif_meta.start_time = date_time_now()
                # Load columns from dict file
                log_info("Args %s %s" % (args, args.columns))
                if args.columns is None:
                    table_columns = {}
                else:
                    table_columns = json.load(open(args.columns, 'r'))
                    table_columns = lower_dict(table_columns)

                log_info(table_columns)

                # Get the column list and file format
                file_format = input_feed['file_format'].lower()
                # This source table name is used to find the schema from the dictionary file
                src_table_name = input_feed['input_schema'].lower()
                table_schema = table_columns[src_table_name] if src_table_name in table_columns.keys() else {}
                log_info("table_schema %s" % table_schema)
                col_list = ",".join(table_schema)
                col_list = col_list  # + ",load_dt string,file_name string"
                log_info("col_list %s" % col_list)

                # Insert data into stage and lake
                # if input_feed['file_format'].lower() in ("csv","txt") and input_feed.get('load_data', 'Y') != 'N':
                if input_feed.get('load_data', 'Y') != 'N':  # not sure why need to test it.
                    ret_code = txt_file_stage(input_feed, hc, sc, properties, env, aif_meta, col_list)
                    if ret_code != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'Reference stage Fail'
                        log_error('hdfs to stage is Fail for table {db}.{tbl}'.format(db=input_feed['target_db'],
                                                                                      tbl=input_feed['target_table']))
                    '''Extra column lists'''
                    if input_feed.get('no_filename', 0) == 0: col_list = col_list + ',file_name string'
                    if input_feed.get('no_access_time', 0) == 0: col_list = col_list + ',access_time string'
                    if input_feed.get('no_modify_time', 0) == 0: col_list = col_list + ',modified_time string'
                    col_list = col_list + ',load_dt string'
                    ret_stage_lake = stage_lake(input_feed, hc, properties, env, aif_meta, col_list)
                    if ret_stage_lake != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'Reference lake Fail'
                        log_error('stage to lake is Fail for table {db}.{tbl}'.format(db=input_feed['lake_db'],
                                                                                      tbl=input_feed['target_table']))

                    # Work DB no longer supported.
                    ret_lake_work = 0
                    # ret_lake_work = lake_work(input_feed, hc, properties, env, aif_meta, col_list)
                    # if ret_lake_work != 0:
                    #    not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                    #    status = 'Reference hub Fail'
                    #    log_error('stage to lake is Fail for table {db}.{tbl}'.format(db=input_feed['hub_db'],
                    #                                                                  tbl=input_feed['target_table']))
                    if ret_code == 0 and ret_stage_lake == 0 and ret_lake_work == 0:
                        status = 'Load Success'
                    aif_meta_write(meta_wb, aif_meta, status)
                else:
                    aif_meta_write(meta_wb, aif_meta, 'Skipped Ingestion')
                log_info("aif_meta.status  : %s" % aif_meta.status)
            elif input_feed['flow_type'] == "xml_hv":
                ''' XML File ingestion to hive tables. 
                Structured XML file with specific root and row tag files'''
                target_db = input_feed['target_db']  # Must have a target_db
                source_db = input_feed["source_db"]
                aif_meta = AIFMeta(input_feed['flow_type'], input_feed['lob'], env, input_feed['load_type'],
                                   getpass.getuser(),
                                   source_db + "/" + input_feed['source_table'], target_db + '.' + input_feed[
                                       'target_table'], input_feed['storage_type'], input_feed['split_column'],
                                   input_feed['lake_partitions'],
                                   string.replace(input_feed['bucket_columns'], ",", "|"),
                                   APP_ID)
                # Starting time
                aif_meta.start_time = date_time_now()
                # Load columns from dict file
                log_info("Args %s %s" % (args, args.columns))
                if args.columns is None:
                    table_columns = {}
                else:
                    table_columns = json.load(open(args.columns, 'r'))
                    table_columns = lower_dict(table_columns)

                log_info(table_columns)

                # Get the column list and file format
                file_format = input_feed['file_format'].lower()
                src_table_name = input_feed[
                    'input_schema'].lower()  # This source table name is used to find the schema from the dictionary file
                table_schema = table_columns[src_table_name] if src_table_name in table_columns.keys() else {}
                log_info("table_schema %s" % table_schema)
                col_list = ",".join(table_schema)
                col_list = col_list  # + ",load_dt string,file_name string"
                log_info("col_list %s" % col_list)

                target_db = input_feed['target_db']  # Must have a target_db
                source_db = input_feed["source_db"]

                file_ptrn = args.file_nm_ptrn if args.file_nm_ptrn != '' else input_feed['source_file'] if input_feed[
                                                                                                               'source_file'] != '' else '*'

                if input_feed.get('local_file', 'Y') != 'N':
                    ret_code = xml_loc_land(input_feed, hc, sc, properties, env, aif_meta, file_ptrn)
                    if ret_code != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'XML file load failed'
                        log_error('XML file ingestion failed ' + str(input_feed['local_dir']))
                    ret_code = xml_land_stage(input_feed, hc, sc, properties, env, aif_meta, file_ptrn, col_list)
                    if ret_code != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'XML file stage load failed'
                        log_error(
                            'XML file land to stage failed ' + input_feed['target_db'] + input_feed['source_table'])
                    ret_code = xml_stage_lake(input_feed, hc, sc, properties, env, aif_meta, col_list)
                    if ret_code != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'XML file lake load failed'
                        log_error('XML file stage to lake failed ' + input_feed['lake_db'] + input_feed['target_table'])
                else:
                    ret_code = xml_land_stage(input_feed, hc, sc, properties, env, aif_meta, file_ptrn, col_list)
                    if ret_code != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'XML file stage load failed'
                        log_error(
                            'XML file land to stage failed ' + input_feed['target_db'] + input_feed['source_table'])
                    ret_code = xml_stage_lake(input_feed, hc, sc, properties, env, aif_meta, col_list)
                    if ret_code != 0:
                        not_loaded_wb.stdin.write(",".join(input_feed.values()) + '\n')
                        status = 'XML file lake load failed'
                        log_error('XML file stage to lake failed ' + input_feed['lake_db'] + input_feed['target_table'])
            else:
                log_info("Input Feed Error: Flow type not correct")
                sys.exit(1)
        else:
            log_info("Skipped table %s" % input_feed['target_table'])

    '''keeps track of not loaded tables. 
    Pull the csv file if table not loaded and rerun with retrieved csv from not loaded path'''
    not_loaded_wb.stdin.close()
    not_loaded_wb.wait()
    rdd = sc.textFile(not_loaded_file_path)
    if rdd.count() > 1:
        log_error("Few or All tables not loaded: Please check %s for more information " % not_loaded_file_path)
        sys.exit(1)
    else:
        delete_hdfs_file(not_loaded_file_path)
    meta_wb.stdin.close()
    meta_wb.wait()
    sc.stop()
