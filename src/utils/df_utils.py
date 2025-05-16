#!/bin/python
from datetime import *
from re import sub
from subprocess import Popen, PIPE, STDOUT
import glob
from pyspark.sql.functions import *
import os
import time

def aif_meta_write(meta_wb,aif_meta, status):
    aif_meta.status = status
    aif_meta.end_time = date_time_now()
    meta_wb.stdin.write(aif_meta.__str__())

def itmp_meta_write(meta_wb,itmp_meta, status):
    itmp_meta.job_status = status
    itmp_meta.job_end_datetime = date_time_now()
    meta_wb.stdin.write(itmp_meta.__str__())
    
def itmp_write(meta_wb,itmp_meta):
    meta_wb.stdin.write(itmp_meta.__str__())
    
def date_time_now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def lower_dict(d):
    new_dict = {k.lower(): v for k, v in d.items()}
    return new_dict


def write_to_hdfs(hdfs_file):
    return Popen(["hadoop", "fs", "-put", "-", hdfs_file], stdin=PIPE, bufsize=-1)


def time_stamped(f_name, fmt='%Y-%m-%d-%H%M%S'):
    return f_name + datetime.now().strftime(fmt)


def date_fun(f_name, fmt='{f_name}_%Y-%m-%d'):
    return date.today().strftime(fmt).format(fname=f_name)


def log_error(err):
    print("LOG :" + date_time_now() + ": ERROR : " + str(err))


def log_info(log):
    print("LOG :" + date_time_now() + ": INFO : " + str(log))


def log_warn(log):
    print("LOG :" + date_time_now() + ": WARN : " + str(log))


def log_debug(log):
    print("LOG :" + date_time_now() + ": DEBUG : " + str(log))


def put_hdfs_file(input_path, output_path):
    res = run_cmd('hadoop fs -put -f ' + input_path + " " + output_path)
    return res.exit_code


def put_hdfs_files(input_path, output_path):
    res = run_cmd('hadoop fs -put ' + input_path + " " + output_path)
    return res.exit_code


def chk_hdfs_file(path):
    res = run_cmd('hadoop fs -test -e ' + path)
    return res.exit_code


def chk_hdfs_dir(path):
    res = run_cmd('hadoop fs -test -d ' + path)
    return res.exit_code


def get_hdfs_files_cnt(path, pattern='*'):
    res = run_cmd("hdfs dfs -find {} -name '{}' | wc -l".format(path, pattern))
    return int(res.stdout.strip('\n').strip('\r').strip())

class Result:
    pass



def run_cmd(cmd):
    result = Result()
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True, universal_newlines=True,
              bufsize=-1, env=dict(os.environ))
    (stdout, stderr) = p.communicate()
    result.exit_code = p.returncode
    result.stdout = stdout
    result.stderr = stderr
    result.cmd = cmd
    if p.returncode != 0:
        print("return code " + str(p.returncode))
        print('Error executing command [%s]' % cmd)
        print('stderr: [%s]' % stderr)
        print('stdout: [%s]' % stdout)
    else:
        print("return code " + str(p.returncode))
        print('stderr: [%s]' % stderr)
        print('command [%s]' % cmd)
        print('stdout: [%s]' % stdout)
    return result

def delete_hdfs_directory(path, skipTrash=False):
    if skipTrash:
        run_cmd("hadoop fs -rm -R -f -skipTrash " + path)
    else:
        run_cmd("hadoop fs -rm -R -f " + path)

def delete_hdfs_file(path):
    run_cmd('hadoop fs -rm ' + path)

def read_hdfs_file_header(sc,file,file_encoding_frmt):
    hadoop = sc._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)
    path = hadoop.fs.Path(file)
    if file_encoding_frmt == 'utf-8-sig':
        header = str(fs.open(path).readLine().encode('utf-8'))
        header = header[6:]
    else:
        header = str(fs.open(path).readLine())
    return header
def get_db_location(db, properties_env):
    location = properties_env['db'][db]
    return location

def get_table_location(db, table, properties_env):
    return get_db_location(db, properties_env) + "/" + table


def gen_table_location(db, table, properties_env):
    return get_db_location(db, properties_env) + "/" + table


def drop_table(db, table, hc, properties_env, deleteData=False):
    if deleteData:
        location = get_table_location(db, table, properties_env)
        hc.sql("drop table if exists " + db + "." + table)
        delete_hdfs_directory(location, True)
    else:
        hc.sql("drop table if exists " + db + "." + table)


def partition_udf(derive_from, format_from, format_to):
    py_format_func = lambda x: sub('jjj', '%j', sub('dd', '%d', sub('mm', '%m',sub('yyyy', '%Y',x.lower()))))
    py_format_func1 = lambda x: sub('dd', '%-d', sub('mm', '%-m',sub('yyyy', '%Y',x.lower())))
    f = py_format_func(format_from)
    t = py_format_func1(format_to)
    return datetime.strptime(str(derive_from).split()[0], f).strftime(t)

def part_fmt_cd_udf(derive_from, format_from, format_to):
    return datetime.strptime(str(derive_from).split()[0], format_from).strftime(format_to)

def get_local_file_list(local_dir, file_name = '', format="csv"):
    '''get the list of files in a local directory as dictionary with metadata last access time, modified time and size.
    input: local_path
    file_name: None or full file name or end with an '*'
    output: list of tuples (file_name, full_path_of_file, access_time and modified_time)
    '''
    try:
        # List all files in directory
        if file_name == '': file_names = [x for x in os.listdir(local_dir) if x.endswith(format) and os.path.isfile(local_dir + '/' + x)]
        # List files that matches with the naming prefix when '*' at the end.
        if file_name != '' and file_name.endswith("*"): file_names = [x for x in os.listdir(local_dir) if x.startswith(file_name[:-1]) and  x.endswith(format)]
        # Check if the file exists
        if file_name != '' and os.path.isfile(local_dir + '/' + file_name) and file_name.endswith(format): file_names = [file_name]
        #return file_names
        if file_names is not None:
            file_list = map(lambda meta_file: [meta_file, os.path.join(local_dir + "/" + meta_file), \
                        datetime.fromtimestamp(os.path.getatime(os.path.join(local_dir + "/" + meta_file))).strftime("%Y-%m-%d %H:%M:%S"), \
                        datetime.fromtimestamp(os.path.getmtime(os.path.join(local_dir + "/" + meta_file))).strftime("%Y-%m-%d %H:%M:%S"), \
                        os.path.getsize(os.path.join(local_dir + "/" + meta_file) )], \
                        file_names)
        return file_list
    except Exception as e:
        log_error("ERROR listing files in local file system")
        log_error(e)


def get_local_files(local_dir, file_name='', format="xml"):
    '''get the list of files in a local directory as dictionary with metadata last access time, modified time and size.
        input: local_path
        file_name: None or full file name or end with an '*'
        output: list of tuples (file_name, full_path_of_file, access_time and modified_time)
    '''
    files = glob.glob('{}{}'.format(local_dir, file_name)) if file_name.endswith(format) else glob.glob('{}{}{}'.format(local_dir, file_name, format))
    return files


def get_hdfs_file_list(sc,hdfs_dir,file_name = ''):
    file_names = None
    try:
        hadoop = sc._jvm.org.apache.hadoop
        fs = hadoop.fs.FileSystem
        conf = hadoop.conf.Configuration()
        source_loc = hdfs_dir
        path = hadoop.fs.Path(source_loc)
        #paths = map(lambda x: [str(x.getPath()).split('/')[-1],str(x.getPath()),datetime.fromtimestamp(x.getAccessTime()).strftime("%Y-%m-%d %H:%M"),datetime.fromtimestamp(x.getModificationTime()).strftime("%Y-%m-%d %H:%M"),str(x.getLen()) ], fs.get(conf).listStatus(path))
        paths = map(lambda x: [str(x.getPath()).split('/')[-1], str(x.getPath()),
                               time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x.getAccessTime() / 1000)) ,
                               time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x.getModificationTime() / 1000)) ,
                               str(x.getLen())], fs.get(conf).listStatus(path))
        source_file_name = file_name
        #All file
        if file_name == '': file_names =  paths
        elif file_name != '' and file_name.endswith("*"): file_names =  filter(lambda x: (source_loc + "/" + source_file_name[:-1]) in x[1], paths)
        elif file_name != '' and '*' not in source_file_name: file_names = filter(lambda x: x[1].endswith(source_loc + "/" + source_file_name), paths)
        if file_names is None: log_error("ERROR file not found in HDFS")
        return file_names
    except Exception as e:
        log_error("ERROR retreiving files from HDFS")
        log_error(e)

def remove_special_char(charlist,string_val):
    charlist = charlist.split(";")
    col_special_char_dict = {}
    for each in charlist:
        dic_data = each.split(":")
        key_element = dic_data[0]
        value_element = dic_data[1]
        col_special_char_dict.update({key_element: value_element})
    for k, v in col_special_char_dict.items():
        string_val = string_val.replace(k, v)
    return string_val