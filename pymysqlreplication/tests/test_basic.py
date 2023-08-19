# -*- coding: utf-8 -*-
import pymysql
import copy
import time
import sys
import io

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from pymysqlreplication.tests import base
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.gtid import GtidSet, Gtid
from pymysqlreplication.event import *
from pymysqlreplication.exceptions import TableMetadataUnavailableError
from pymysqlreplication.constants.BINLOG import *
from pymysqlreplication.row_event import *

__all__ = ["TestBasicBinLogStreamReader", "TestMultipleRowBinLogStreamReader", "TestCTLConnectionSettings",
           "TestGtidBinLogStreamReader", "TestOptionalMetaData"]


class TestBasicBinLogStreamReader(base.PyMySQLReplicationTestCase):
    def ignoredEvents(self):
        return [GtidEvent]

    def test_allowed_event_list(self):
        self.assertEqual(len(self.stream._allowed_event_list(None, None, False)), 16)
        self.assertEqual(len(self.stream._allowed_event_list(None, None, True)), 15)
        self.assertEqual(len(self.stream._allowed_event_list(None, [RotateEvent], False)), 15)
        self.assertEqual(len(self.stream._allowed_event_list([RotateEvent], None, False)), 1)

    def test_read_query_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        event = self.stream.fetchone()
        self.assertEqual(event.position, 4)
        self.assertEqual(event.next_binlog, self.bin_log_basename() + ".000001")
        self.assertIsInstance(event, RotateEvent)

        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        event = self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

    def test_read_query_event_with_unicode(self):
        query = u"CREATE TABLE `testÈ` (id INT NOT NULL AUTO_INCREMENT, dataÈ VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        event = self.stream.fetchone()
        self.assertEqual(event.position, 4)
        self.assertEqual(event.next_binlog, self.bin_log_basename() + ".000001")
        self.assertIsInstance(event, RotateEvent)

        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        event = self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

    def test_reading_rotate_event(self):
        query = "CREATE TABLE test_2 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.stream.close()

        query = "CREATE TABLE test_3 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        # Rotate event
        self.assertIsInstance(self.stream.fetchone(), RotateEvent)

    """ `test_load_query_event` needs statement-based binlog
    def test_load_query_event(self):
        # prepare csv
        with open("/tmp/test_load_query.csv", "w") as fp:
            fp.write("1,aaa\n2,bbb\n3,ccc\n4,ddd\n")

        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "LOAD DATA INFILE '/tmp/test_load_query.csv' INTO TABLE test \
                FIELDS TERMINATED BY ',' \
                ENCLOSED BY '\"' \
                LINES TERMINATED BY '\r\n'"
        self.execute(query)

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        # create table
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        # begin
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), BeginLoadQueryEvent)
        self.assertIsInstance(self.stream.fetchone(), ExecuteLoadQueryEvent)

        self.assertIsInstance(self.stream.fetchone(), XidEvent)
    """

    def test_connection_stream_lost_event(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, blocking=True,
            ignored_events=self.ignoredEvents())

        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query2 = "INSERT INTO test (data) VALUES('a')"
        for i in range(0, 10000):
            self.execute(query2)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        event = self.stream.fetchone()

        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

        self.conn_control.kill(self.stream._stream_connection.thread_id())
        for i in range(0, 10000):
            event = self.stream.fetchone()
            self.assertIsNotNone(event)

    def test_filtering_only_events(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, only_events=[QueryEvent])
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        event = self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

    def test_filtering_ignore_events(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, ignored_events=[QueryEvent])
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        event = self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)

    def test_filtering_table_event_with_only_tables(self):
        self.stream.close()
        self.assertEqual(self.bin_log_format(), "ROW")
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=[WriteRowsEvent],
            only_tables=["test_2"]
        )

        query = "CREATE TABLE test_2 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "CREATE TABLE test_3 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        self.execute("INSERT INTO test_2 (data) VALUES ('alpha')")
        self.execute("INSERT INTO test_3 (data) VALUES ('alpha')")
        self.execute("INSERT INTO test_2 (data) VALUES ('beta')")
        self.execute("COMMIT")
        event = self.stream.fetchone()
        self.assertEqual(event.table, "test_2")
        event = self.stream.fetchone()
        self.assertEqual(event.table, "test_2")

    def test_filtering_table_event_with_ignored_tables(self):
        self.stream.close()
        self.assertEqual(self.bin_log_format(), "ROW")
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=[WriteRowsEvent],
            ignored_tables=["test_2"]
        )

        query = "CREATE TABLE test_2 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "CREATE TABLE test_3 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        self.execute("INSERT INTO test_2 (data) VALUES ('alpha')")
        self.execute("INSERT INTO test_3 (data) VALUES ('alpha')")
        self.execute("INSERT INTO test_2 (data) VALUES ('beta')")
        self.execute("COMMIT")
        event = self.stream.fetchone()
        self.assertEqual(event.table, "test_3")

    def test_filtering_table_event_with_only_tables_and_ignored_tables(self):
        self.stream.close()
        self.assertEqual(self.bin_log_format(), "ROW")
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=[WriteRowsEvent],
            only_tables=["test_2"],
            ignored_tables=["test_3"]
        )

        query = "CREATE TABLE test_2 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "CREATE TABLE test_3 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        self.execute("INSERT INTO test_2 (data) VALUES ('alpha')")
        self.execute("INSERT INTO test_3 (data) VALUES ('alpha')")
        self.execute("INSERT INTO test_2 (data) VALUES ('beta')")
        self.execute("COMMIT")
        event = self.stream.fetchone()
        self.assertEqual(event.table, "test_2")

    def test_write_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        # QueryEvent for the Create Table
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V1)
        self.assertIsInstance(event, WriteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello World")
        self.assertEqual(event.schema, "pymysqlreplication_test")
        self.assertEqual(event.table, "test")
        self.assertEqual(event.columns[1].name, 'data')

    def test_delete_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        self.execute(query)

        self.resetBinLog()

        query = "DELETE FROM test WHERE id = 1"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V1)
        self.assertIsInstance(event, DeleteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello World")

    def test_update_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        self.execute(query)

        self.resetBinLog()

        query = "UPDATE test SET data = 'World' WHERE id = 1"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V1)
        self.assertIsInstance(event, UpdateRowsEvent)
        self.assertEqual(event.rows[0]["before_values"]["id"], 1)
        self.assertEqual(event.rows[0]["before_values"]["data"], "Hello")
        self.assertEqual(event.rows[0]["after_values"]["id"], 1)
        self.assertEqual(event.rows[0]["after_values"]["data"], "World")

    def test_minimal_image_write_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "SET SESSION binlog_row_image = 'minimal'"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        # QueryEvent for the Create Table
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V1)
        self.assertIsInstance(event, WriteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello World")
        self.assertEqual(event.schema, "pymysqlreplication_test")
        self.assertEqual(event.table, "test")
        self.assertEqual(event.columns[1].name, 'data')

    def test_minimal_image_delete_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        self.execute(query)
        query = "SET SESSION binlog_row_image = 'minimal'"
        self.execute(query)
        self.resetBinLog()

        query = "DELETE FROM test WHERE id = 1"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V1)
        self.assertIsInstance(event, DeleteRowsEvent)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], None)

    def test_minimal_image_update_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        self.execute(query)
        query = "SET SESSION binlog_row_image = 'minimal'"
        self.execute(query)
        self.resetBinLog()

        query = "UPDATE test SET data = 'World' WHERE id = 1"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V1)
        self.assertIsInstance(event, UpdateRowsEvent)
        self.assertEqual(event.rows[0]["before_values"]["id"], 1)
        self.assertEqual(event.rows[0]["before_values"]["data"], None)
        self.assertEqual(event.rows[0]["after_values"]["id"], None)
        self.assertEqual(event.rows[0]["after_values"]["data"], "World")

    def test_log_pos(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        self.execute(query)
        self.execute("COMMIT")

        for i in range(6):
            self.stream.fetchone()
        # record position after insert
        log_file, log_pos = self.stream.log_file, self.stream.log_pos

        query = "UPDATE test SET data = 'World' WHERE id = 1"
        self.execute(query)
        self.execute("COMMIT")

        # resume stream from previous position
        if self.stream is not None:
            self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            resume_stream=True,
            log_file=log_file,
            log_pos=log_pos,
            ignored_events=self.ignoredEvents()
        )

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        self.assertIsInstance(self.stream.fetchone(), XidEvent)
        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)
        self.assertIsInstance(self.stream.fetchone(), UpdateRowsEvent)
        self.assertIsInstance(self.stream.fetchone(), XidEvent)

    def test_log_pos_handles_disconnects(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            resume_stream=False,
            only_events=[FormatDescriptionEvent, QueryEvent, TableMapEvent, WriteRowsEvent, XidEvent]
        )

        query = "CREATE TABLE test (id INT  PRIMARY KEY AUTO_INCREMENT, data VARCHAR (50) NOT NULL)"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        self.assertGreater(self.stream.log_pos, 0)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)
        self.assertIsInstance(self.stream.fetchone(), WriteRowsEvent)

        self.assertIsInstance(self.stream.fetchone(), XidEvent)

        self.assertGreater(self.stream.log_pos, 0)

    def test_skip_to_timestamp(self):
        self.stream.close()
        query = "CREATE TABLE test_1 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        time.sleep(1)
        query = "SELECT UNIX_TIMESTAMP();"
        timestamp = self.execute(query).fetchone()[0]
        query2 = "CREATE TABLE test_2 (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query2)

        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            skip_to_timestamp=timestamp,
            ignored_events=self.ignoredEvents(),
        )
        event = self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query2)

    def test_end_log_pos(self):
        """Test end_log_pos parameter for BinLogStreamReader

        MUST BE TESTED IN DEFAULT SYSTEM VARIABLES SETTING

        Raises:
            AssertionError: if null_bitmask isn't set as specified in 'bit_mask' variable
        """

        self.execute('CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))')
        self.execute('INSERT INTO test values (NULL)')
        self.execute('INSERT INTO test values (NULL)')
        self.execute('INSERT INTO test values (NULL)')
        self.execute('INSERT INTO test values (NULL)')
        self.execute('INSERT INTO test values (NULL)')
        self.execute('COMMIT')
        # import os
        # os._exit(1)

        binlog = self.execute("SHOW BINARY LOGS").fetchone()[0]

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            log_pos=0,
            log_file=binlog,
            end_log_pos=888)

        last_log_pos = 0
        last_event_type = 0
        for event in self.stream:
            last_log_pos = self.stream.log_pos
            last_event_type = event.event_type

        self.assertEqual(last_log_pos, 888)
        self.assertEqual(last_event_type, TABLE_MAP_EVENT)


class TestMultipleRowBinLogStreamReader(base.PyMySQLReplicationTestCase):
    def ignoredEvents(self):
        return [GtidEvent]

    def test_insert_multiple_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        self.resetBinLog()

        query = "INSERT INTO test (data) VALUES('Hello'),('World')"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V1)
        self.assertIsInstance(event, WriteRowsEvent)
        self.assertEqual(len(event.rows), 2)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello")

        self.assertEqual(event.rows[1]["values"]["id"], 2)
        self.assertEqual(event.rows[1]["values"]["data"], "World")

    def test_update_multiple_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('World')"
        self.execute(query)

        self.resetBinLog()

        query = "UPDATE test SET data = 'Toto'"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V1)
        self.assertIsInstance(event, UpdateRowsEvent)
        self.assertEqual(len(event.rows), 2)
        self.assertEqual(event.rows[0]["before_values"]["id"], 1)
        self.assertEqual(event.rows[0]["before_values"]["data"], "Hello")
        self.assertEqual(event.rows[0]["after_values"]["id"], 1)
        self.assertEqual(event.rows[0]["after_values"]["data"], "Toto")

        self.assertEqual(event.rows[1]["before_values"]["id"], 2)
        self.assertEqual(event.rows[1]["before_values"]["data"], "World")
        self.assertEqual(event.rows[1]["after_values"]["id"], 2)
        self.assertEqual(event.rows[1]["after_values"]["data"], "Toto")

    def test_delete_multiple_row_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello')"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('World')"
        self.execute(query)

        self.resetBinLog()

        query = "DELETE FROM test"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, DELETE_ROWS_EVENT_V1)
        self.assertIsInstance(event, DeleteRowsEvent)
        self.assertEqual(len(event.rows), 2)
        self.assertEqual(event.rows[0]["values"]["id"], 1)
        self.assertEqual(event.rows[0]["values"]["data"], "Hello")

        self.assertEqual(event.rows[1]["values"]["id"], 2)
        self.assertEqual(event.rows[1]["values"]["data"], "World")

    def test_drop_table(self):
        self.execute("CREATE TABLE test (id INTEGER(11))")
        self.execute("INSERT INTO test VALUES (1)")
        self.execute("DROP TABLE test")
        self.execute("COMMIT")

        # RotateEvent
        self.stream.fetchone()
        # FormatDescription
        self.stream.fetchone()
        # QueryEvent for the Create Table
        self.stream.fetchone()

        # QueryEvent for the BEGIN
        self.stream.fetchone()

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, WRITE_ROWS_EVENT_V1)
        self.assertIsInstance(event, WriteRowsEvent)

        self.assertEqual([], event.rows)

    def test_drop_table_tablemetadata_unavailable(self):
        self.stream.close()
        self.execute("CREATE TABLE test (id INTEGER(11))")
        self.execute("INSERT INTO test VALUES (1)")
        self.execute("DROP TABLE test")
        self.execute("COMMIT")

        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
            fail_on_table_metadata_unavailable=True
        )
        had_error = False
        try:
            event = self.stream.fetchone()
        except TableMetadataUnavailableError as e:
            had_error = True
            assert "test" in e.args[0]
        finally:
            self.resetBinLog()
            assert had_error

    def test_ignore_decode_errors(self):
        problematic_unicode_string = b'[{"text":"\xed\xa0\xbd \xed\xb1\x8d Some string"}]'
        self.stream.close()
        self.execute("CREATE TABLE test (data VARCHAR(50) CHARACTER SET utf8mb4)")
        self.execute_with_args("INSERT INTO test (data) VALUES (%s)", (problematic_unicode_string))
        self.execute("COMMIT")

        # Initialize with ignore_decode_errors=False
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
            ignore_decode_errors=False
        )
        event = self.stream.fetchone()
        event = self.stream.fetchone()
        with self.assertRaises(UnicodeError) as exception:
            event = self.stream.fetchone()
            data = event.rows[0]["values"]["data"]

        # Initialize with ignore_decode_errors=True
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
            ignore_decode_errors=True
        )
        self.stream.fetchone()
        self.stream.fetchone()
        event = self.stream.fetchone()
        data = event.rows[0]["values"]["data"]
        self.assertEqual(data, '[{"text":"  Some string"}]')

    def test_drop_column(self):
        self.stream.close()
        self.execute("CREATE TABLE test_drop_column (id INTEGER(11), data VARCHAR(50))")
        self.execute("INSERT INTO test_drop_column VALUES (1, 'A value')")
        self.execute("COMMIT")
        self.execute("ALTER TABLE test_drop_column DROP COLUMN data")
        self.execute("INSERT INTO test_drop_column VALUES (2)")
        self.execute("COMMIT")

        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,)
        )
        try:
            self.stream.fetchone()  # insert with two values
            self.stream.fetchone()  # insert with one value
        except Exception as e:
            self.fail("raised unexpected exception: {exception}".format(exception=e))
        finally:
            self.resetBinLog()

    @unittest.expectedFailure
    def test_alter_column(self):
        self.stream.close()
        self.execute("CREATE TABLE test_alter_column (id INTEGER(11), data VARCHAR(50))")
        self.execute("INSERT INTO test_alter_column VALUES (1, 'A value')")
        self.execute("COMMIT")
        # this is a problem only when column is added in position other than at the end
        self.execute("ALTER TABLE test_alter_column ADD COLUMN another_data VARCHAR(50) AFTER id")
        self.execute("INSERT INTO test_alter_column VALUES (2, 'Another value', 'A value')")
        self.execute("COMMIT")

        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
        )
        event = self.stream.fetchone()  # insert with two values
        # both of these asserts fail because of issue underlying proble described in issue #118
        # because it got table schema info after the alter table, it wrongly assumes the second
        # column of the first insert is 'another_data'
        # ER: {'id': 1, 'data': 'A value'}
        # AR: {'id': 1, 'another_data': 'A value'}
        self.assertIn("data", event.rows[0]["values"])
        self.assertNot("another_data", event.rows[0]["values"])
        self.assertEqual(event.rows[0]["values"]["data"], 'A value')
        self.stream.fetchone()  # insert with three values


class TestCTLConnectionSettings(base.PyMySQLReplicationTestCase):

    def setUp(self):
        super(TestCTLConnectionSettings, self).setUp()
        self.stream.close()
        ctl_db = copy.copy(self.database)
        ctl_db["db"] = None
        ctl_db["port"] = 3307
        self.ctl_conn_control = pymysql.connect(**ctl_db)
        self.ctl_conn_control.cursor().execute("DROP DATABASE IF EXISTS pymysqlreplication_test")
        self.ctl_conn_control.cursor().execute("CREATE DATABASE pymysqlreplication_test")
        self.ctl_conn_control.close()
        ctl_db["db"] = "pymysqlreplication_test"
        self.ctl_conn_control = pymysql.connect(**ctl_db)
        self.stream = BinLogStreamReader(
            self.database,
            ctl_connection_settings=ctl_db,
            server_id=1024,
            only_events=(WriteRowsEvent,),
            fail_on_table_metadata_unavailable=True
        )

    def tearDown(self):
        super(TestCTLConnectionSettings, self).tearDown()
        self.ctl_conn_control.close()

    def test_separate_ctl_settings_table_metadata_unavailable(self):
        self.execute("CREATE TABLE test (id INTEGER(11))")
        self.execute("INSERT INTO test VALUES (1)")
        self.execute("COMMIT")

        had_error = False
        try:
            event = self.stream.fetchone()
        except TableMetadataUnavailableError as e:
            had_error = True
            assert "test" in e.args[0]
        finally:
            self.resetBinLog()
            assert had_error

    def test_separate_ctl_settings_no_error(self):
        self.execute("CREATE TABLE test (id INTEGER(11))")
        self.execute("INSERT INTO test VALUES (1)")
        self.execute("DROP TABLE test")
        self.execute("COMMIT")
        self.ctl_conn_control.cursor().execute("CREATE TABLE test (id INTEGER(11))")
        self.ctl_conn_control.cursor().execute("INSERT INTO test VALUES (1)")
        self.ctl_conn_control.cursor().execute("COMMIT")
        try:
            self.stream.fetchone()
        except Exception as e:
            self.fail("raised unexpected exception: {exception}".format(exception=e))
        finally:
            self.resetBinLog()

class TestGtidBinLogStreamReader(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestGtidBinLogStreamReader, self).setUp()
        if not self.supportsGTID:
            raise unittest.SkipTest("database does not support GTID, skipping GTID tests")

    def test_read_query_event(self):
        query = "CREATE TABLE test (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "SELECT @@global.gtid_executed;"
        gtid = self.execute(query).fetchone()[0]

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, blocking=True, auto_position=gtid,
            ignored_events=[HeartbeatLogEvent])

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        # Insert first event
        query = "BEGIN;"
        self.execute(query)
        query = "INSERT INTO test (id, data) VALUES(1, 'Hello');"
        self.execute(query)
        query = "COMMIT;"
        self.execute(query)

        firstevent = self.stream.fetchone()
        self.assertIsInstance(firstevent, GtidEvent)

        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)
        self.assertIsInstance(self.stream.fetchone(), WriteRowsEvent)
        self.assertIsInstance(self.stream.fetchone(), XidEvent)

        # Insert second event
        query = "BEGIN;"
        self.execute(query)
        query = "INSERT INTO test (id, data) VALUES(2, 'Hello');"
        self.execute(query)
        query = "COMMIT;"
        self.execute(query)

        secondevent = self.stream.fetchone()
        self.assertIsInstance(secondevent, GtidEvent)

        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)
        self.assertIsInstance(self.stream.fetchone(), WriteRowsEvent)
        self.assertIsInstance(self.stream.fetchone(), XidEvent)

        self.assertEqual(secondevent.gno, firstevent.gno + 1)

    def test_position_gtid(self):
        query = "CREATE TABLE test (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "BEGIN;"
        self.execute(query)
        query = "INSERT INTO test (id, data) VALUES(1, 'Hello');"
        self.execute(query)
        query = "COMMIT;"
        self.execute(query)

        query = "SELECT @@global.gtid_executed;"
        gtid = self.execute(query).fetchone()[0]

        query = "CREATE TABLE test2 (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, blocking=True, auto_position=gtid,
            ignored_events=[HeartbeatLogEvent])

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        self.assertIsInstance(self.stream.fetchone(), GtidEvent)
        event = self.stream.fetchone()

        self.assertEqual(event.query,
                         'CREATE TABLE test2 (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))');


class TestGtidRepresentation(unittest.TestCase):
    def test_gtidset_representation(self):
        set_repr = '57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,' \
                   '4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20'

        myset = GtidSet(set_repr)
        self.assertEqual(str(myset), set_repr)

    def test_gtidset_representation_newline(self):
        set_repr = '57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,' \
                   '4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20'
        mysql_repr = '57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,\n' \
                     '4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20'

        myset = GtidSet(mysql_repr)
        self.assertEqual(str(myset), set_repr)

    def test_gtidset_representation_payload(self):
        set_repr = '57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,' \
                   '4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20'

        myset = GtidSet(set_repr)
        payload = myset.encode()
        parsedset = myset.decode(io.BytesIO(payload))

        self.assertEqual(str(myset), str(parsedset))

        set_repr = '57b70f4e-20d3-11e5-a393-4a63946f7eac:1,' \
                   '4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20'

        myset = GtidSet(set_repr)
        payload = myset.encode()
        parsedset = myset.decode(io.BytesIO(payload))

        self.assertEqual(str(myset), str(parsedset))


class GtidTests(unittest.TestCase):
    def test_ordering(self):
        gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56")
        other = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:5-10")
        assert gtid.__lt__(other)
        assert gtid.__le__(other)
        assert other.__gt__(gtid)
        assert other.__ge__(gtid)
        gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56")
        other = Gtid("deadbeef-20d3-11e5-a393-4a63946f7eac:5-10")
        assert gtid.__lt__(other)
        assert gtid.__le__(other)
        assert other.__gt__(gtid)
        assert other.__ge__(gtid)

    def test_encode_decode(self):
        gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56")
        payload = gtid.encode()
        decoded = Gtid.decode(io.BytesIO(payload))
        assert str(gtid) == str(decoded)

    def test_add_interval(self):
        gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:5-56")
        end = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:57-58")
        assert (gtid + end).intervals == [(5, 59)]

        start = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-2")
        assert (gtid + start).intervals == [(1, 3), (5, 57)]

        sparse = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-4:7-10")
        within = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:5-6")
        assert (sparse + within).intervals == [(1, 11)]

    def test_interval_non_merging(self):
        gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56")
        other = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:58-59")
        gtid = gtid + other
        self.assertEqual(str(gtid), "57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56:58-59")

    def test_merging(self):
        gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56")
        other = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:57-59")
        gtid = gtid + other
        self.assertEqual(str(gtid), "57b70f4e-20d3-11e5-a393-4a63946f7eac:1-59")

    def test_sub_interval(self):
        gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56")
        start = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-5")
        assert (gtid - start).intervals == [(6, 57)]

        end = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:55-56")
        assert (gtid - end).intervals == [(1, 55)]

        within = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:25-26")
        assert (gtid - within).intervals == [(1, 25), (27, 57)]

    def test_parsing(self):
        with self.assertRaises(ValueError) as exc:
            gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-5 57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56")
            gtid = Gtid("NNNNNNNN-20d3-11e5-a393-4a63946f7eac:1-5")
            gtid = Gtid("-20d3-11e5-a393-4a63946f7eac:1-5")
            gtid = Gtid("-20d3-11e5-a393-4a63946f7eac:1-")
            gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:A-1")
            gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:-1")
            gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-:1")
            gtid = Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac::1")


class TestOptionalMetaData(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestOptionalMetaData, self).setUp()
        self.stream.close()
        optional_metadata_db = copy.copy(self.database)
        optional_metadata_db["db"] = None
        optional_metadata_db["port"] = 3308
        self.optional_metadata_conn_control = pymysql.connect(**optional_metadata_db)
        self.optional_metadata_conn_control.cursor().execute("DROP DATABASE IF EXISTS pymysqlreplication_test")
        self.optional_metadata_conn_control.cursor().execute("CREATE DATABASE pymysqlreplication_test")
        self.optional_metadata_conn_control.close()
        optional_metadata_db["db"] = "pymysqlreplication_test"
        self.optional_metadata_conn_control = pymysql.connect(**optional_metadata_db)
        self.stream = BinLogStreamReader(
            self.database,
            ctl_connection_settings=optional_metadata_db,
            server_id=1024,
            only_events=(TableMapEvent,),
            fail_on_table_metadata_unavailable=True
        )
        self.isMySQL8014AndMore()
        self.execute("SET GLOBAL binlog_row_metadata = 'FULL'")

    def test_signedness(self):
        create_query = "CREATE TABLE test_signedness (col1 INT, col2 INT UNSIGNED)"
        insert_query = "INSERT INTO test_signedness VALUES (-10, 10)"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        expected_table_map_event = self.stream.fetchone()
        self.assertIsInstance(expected_table_map_event, TableMapEvent)
        self.assertEqual(expected_table_map_event.optional_metadata.unsigned_column_list, [False, True])

    def test_set_str_value(self):
        query = "CREATE TABLE test_set (skills SET('Programming', 'Writing', 'Design'));"
        self.execute(query)
        query = "BEGIN"
        self.execute(query)
        query = "INSERT INTO test_set VALUES ('Programming,Writing');"
        self.execute(query)
        query = "COMMIT"
        self.execute(query)

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(event.optional_metadata.set_str_value_list, [['Programming', 'Writing', 'Design']])

    def test_enum_str_value(self):
        query = "CREATE TABLE test_enum (pet ENUM('Dog', 'Cat'));"
        self.execute(query)
        query = "BEGIN"
        self.execute(query)
        query = "INSERT INTO test_enum VALUES ('Cat');"
        self.execute(query)
        query = "COMMIT"
        self.execute(query)

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(event.optional_metadata.set_enum_str_value_list, [['Dog', 'Cat']])

    def test_simple_primary_keys(self):
        create_query = "CREATE TABLE test_simple (c_key1 INT, c_not_key INT, c_key2 INT, PRIMARY KEY(c_key1, c_key2));"
        insert_query = "INSERT INTO test_simple VALUES (1, 2, 3);"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        table_map_event = self.stream.fetchone()
        self.assertIsInstance(table_map_event, TableMapEvent)
        self.assertEqual(table_map_event.optional_metadata.simple_primary_key_list, [0, 2])


    def test_primary_keys_with_prefix(self):
        create_query = "CREATE TABLE t2(c_key1 CHAR(100), c_key3 CHAR(100), c_not_key INT, c_key2 CHAR(10),PRIMARY KEY(c_key1(5), c_key2, c_key3(10)));"
        insert_query = "INSERT INTO t2 VALUES('1', '2', 3, '4');"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        table_map_event = self.stream.fetchone()
        self.assertIsInstance(table_map_event, TableMapEvent)
        self.assertEqual(table_map_event.optional_metadata.primary_keys_with_prefix, {0: 5, 1: 10, 3: 0})

    def tearDown(self):
        self.execute("SET GLOBAL binlog_row_metadata='MINIMAL';")
        super(TestOptionalMetaData, self).tearDown()
        self.optional_metadata_conn_control.close()

if __name__ == "__main__":
    import unittest
    unittest.main()
