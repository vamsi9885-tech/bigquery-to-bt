#!/bin/python
#####################################################
# File Name: custom_logging.py
# Type: Pyspark
# Purpose: To enable logging.
# Created: 21/07/2020
# Last Updated: 21/07/2020
# Author: CDP Team
#####################################################


class Log4j(object):
    """Wrapper class for Log4j JVM object.
    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')
        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, module_name, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(module_name + " " + message)
        return None

    def warn(self, module_name, message):
        """Log an warning.
        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(module_name + " " + message)
        return None

    def info(self, module_name, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(module_name + " " + message)
        return None
