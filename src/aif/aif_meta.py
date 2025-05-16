from collections import OrderedDict


class AIFMeta:

    def __init__(self, connection_type, lob, environment, load_type, user_name, source_name, target_name, serde,
                 split_by, partition_columns, bucketing_columns, app_id):
        self.__dict__ = OrderedDict()
        self.__dict__['connection_type'] = connection_type
        self.__dict__['lob'] = lob
        self.__dict__['environment'] = environment
        self.__dict__['load_type'] = load_type
        self.__dict__['user_name'] = user_name
        self.__dict__['source_name'] = source_name
        self.__dict__['target_name'] = target_name
        self.__dict__['serde'] = serde
        self.__dict__['split_by'] = split_by
        self.__dict__['partition_columns'] = partition_columns
        self.__dict__['bucketing_columns'] = bucketing_columns
        self.__dict__['source_count'] = "0"
        self.__dict__['target_count'] = "0"
        self.__dict__['start_time'] = ""
        self.__dict__['end_time'] = ""
        self.__dict__['status'] = ""
        self.__dict__['data_size_mb'] = "NA"
        self.__dict__['app_id'] = app_id

    def __str__(self):
        return ','.join(['{value}'.format(value=self.__dict__.get(key)) for key in self.__dict__]) + '\n'
