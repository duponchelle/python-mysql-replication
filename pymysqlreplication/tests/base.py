import pymysql
import copy
from pymysqlreplication import BinLogStreamReader
import os
import json
import pytest

import unittest


def get_databases():
    databases = {}
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    ) as f:
        databases = json.load(f)
    return databases


base = unittest.TestCase


class PyMySQLReplicationTestCase(base):
    def ignoredEvents(self):
        return []

    @pytest.fixture(autouse=True)
    def setUpDatabase(self, get_db):
        databases = get_databases()
        # For local testing, set the get_dbms parameter to one of the following values: 'mysql-5', 'mysql-8', mariadb-10'.
        # This value should correspond to the desired database configuration specified in the 'config.json' file.
        self.database = databases[get_db]
        """
        self.database = {
            "host": os.environ.get("MYSQL_5_7") or "localhost",
            "user": "root",
            "passwd": "",
            "port": 3306,
            "use_unicode": True,
            "charset": charset,
            "db": "pymysqlreplication_test",
        }
        """

    def setUp(self, charset="utf8"):
        # default
        self.conn_control = None
        db = copy.copy(self.database)
        db["db"] = None
        self.connect_conn_control(db)
        self.execute("DROP DATABASE IF EXISTS pymysqlreplication_test")
        self.execute("CREATE DATABASE pymysqlreplication_test")
        db = copy.copy(self.database)
        db["charset"] = charset
        self.connect_conn_control(db)
        self.stream = None
        self.resetBinLog()
        self.isMySQL56AndMore()
        self.__is_mariaDB = None

    def getMySQLVersion(self):
        """Return the MySQL version of the server
        If version is 5.6.10-log the result is 5.6.10
        """
        return self.execute("SELECT VERSION()").fetchone()[0].split("-")[0]

    def isMySQL56AndMore(self):
        version = float(self.getMySQLVersion().rsplit(".", 1)[0])
        if version >= 5.6:
            return True
        return False

    def isMySQL57(self):
        version = float(self.getMySQLVersion().rsplit(".", 1)[0])
        return version == 5.7

    def isMySQL801AndMore(self):
        version = float(self.getMySQLVersion().rsplit(".", 1)[0])
        version_detail = int(self.getMySQLVersion().rsplit(".", 1)[1])
        if version > 8.0:
            return True
        return version == 8.0 and version_detail >= 10

    def isMySQL80AndMore(self):
        version = float(self.getMySQLVersion().rsplit(".", 1)[0])
        return version >= 8.0

    def isMySQL8014AndMore(self):
        version = float(self.getMySQLVersion().rsplit(".", 1)[0])
        version_detail = int(self.getMySQLVersion().rsplit(".", 1)[1])
        if version > 8.0:
            return True
        return version == 8.0 and version_detail >= 14

    def isMySQL8016AndMore(self):
        version = float(self.getMySQLVersion().rsplit(".", 1)[0])
        version_detail = int(self.getMySQLVersion().rsplit(".", 1)[1])
        if version > 8.0:
            return True
        return version == 8.0 and version_detail >= 16

    def isMariaDB(self):
        if self.__is_mariaDB is None:
            self.__is_mariaDB = (
                "MariaDB" in self.execute("SELECT VERSION()").fetchone()[0]
            )
        return self.__is_mariaDB

    @property
    def supportsGTID(self):
        if not self.isMySQL56AndMore():
            return False
        return self.execute("SELECT @@global.gtid_mode ").fetchone()[0] == "ON"

    def connect_conn_control(self, db):
        if self.conn_control is not None:
            self.conn_control.close()
        self.conn_control = pymysql.connect(**db)

    def tearDown(self):
        self.conn_control.close()
        self.conn_control = None
        self.stream.close()
        self.stream = None

    def execute(self, query):
        c = self.conn_control.cursor()
        c.execute(query)
        return c

    def execute_with_args(self, query, args):
        c = self.conn_control.cursor()
        c.execute(query, args)
        return c

    def resetBinLog(self):
        self.execute("RESET MASTER")
        if self.stream is not None:
            self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, ignored_events=self.ignoredEvents()
        )

    def set_sql_mode(self):
        """set sql_mode to test with same sql_mode (mysql 5.7 sql_mode default is changed)"""
        version = float(self.getMySQLVersion().rsplit(".", 1)[0])
        if version == 5.7:
            self.execute("SET @@sql_mode='NO_ENGINE_SUBSTITUTION'")

    def bin_log_format(self):
        query = "SELECT @@binlog_format"
        cursor = self.execute(query)
        result = cursor.fetchone()
        return result[0]

    def bin_log_basename(self):
        cursor = self.execute("SELECT @@log_bin_basename")
        bin_log_basename = cursor.fetchone()[0]
        bin_log_basename = bin_log_basename.split("/")[-1]
        return bin_log_basename
