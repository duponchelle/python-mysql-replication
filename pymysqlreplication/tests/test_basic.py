import io
import time
import unittest

from pymysqlreplication.json_binary import JsonDiff, JsonDiffOperation
from pymysqlreplication.tests import base
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.gtid import GtidSet, Gtid
from pymysqlreplication.event import *
from pymysqlreplication.constants.BINLOG import *
from pymysqlreplication.constants.NONE_SOURCE import *
from pymysqlreplication.row_event import *
from pymysqlreplication.packet import BinLogPacketWrapper
from pymysql.protocol import MysqlPacket
from unittest.mock import patch
import pytest

__all__ = [
    "TestBasicBinLogStreamReader",
    "TestMultipleRowBinLogStreamReader",
    "TestCTLConnectionSettings",
    "TestGtidBinLogStreamReader",
    "TestMariadbBinlogStreamReader",
    "TestStatementConnectionSetting",
    "TestRowsQueryLogEvents",
    "TestOptionalMetaData",
    "TestColumnValueNoneSources",
    "TestJsonPartialUpdate",
]


class TestBasicBinLogStreamReader(base.PyMySQLReplicationTestCase):
    def ignoredEvents(self):
        return [GtidEvent, PreviousGtidsEvent]

    def test_allowed_event_list(self):
        self.assertEqual(len(self.stream._allowed_event_list(None, None, False)), 25)
        self.assertEqual(len(self.stream._allowed_event_list(None, None, True)), 24)
        self.assertEqual(
            len(self.stream._allowed_event_list(None, [RotateEvent], False)), 24
        )
        self.assertEqual(
            len(self.stream._allowed_event_list([RotateEvent], None, False)), 1
        )

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
        query = "CREATE TABLE `testÈ` (id INT NOT NULL AUTO_INCREMENT, dataÈ VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
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
            self.database,
            server_id=1024,
            blocking=True,
            ignored_events=self.ignoredEvents(),
        )

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
            self.database, server_id=1024, only_events=[QueryEvent]
        )
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)

        event = self.stream.fetchone()
        self.assertIsInstance(event, QueryEvent)
        self.assertEqual(event.query, query)

    def test_filtering_ignore_events(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, ignored_events=[QueryEvent]
        )
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
            only_tables=["test_2"],
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
            ignored_tables=["test_2"],
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
            ignored_tables=["test_3"],
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
        self.assertEqual(event.schema, "pymysqlreplication_test")
        self.assertEqual(event.table, "test")
        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(event.rows[0]["values"]["id"], 1)
            self.assertEqual(event.rows[0]["values"]["data"], "Hello World")
            self.assertEqual(event.columns[1].name, "data")

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
        if event.table_map[event.table_id].column_name_flag:
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
        if event.table_map[event.table_id].column_name_flag:
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
        self.assertEqual(event.schema, "pymysqlreplication_test")
        self.assertEqual(event.table, "test")
        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(event.columns[1].name, "data")
            self.assertEqual(event.rows[0]["values"]["id"], 1)
            self.assertEqual(event.rows[0]["values"]["data"], "Hello World")

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
        if event.table_map[event.table_id].column_name_flag:
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
        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(event.rows[0]["before_values"]["id"], 1)
            self.assertEqual(event.rows[0]["before_values"]["data"], None)
            self.assertEqual(event.rows[0]["after_values"]["id"], None)
            self.assertEqual(event.rows[0]["after_values"]["data"], "World")

    def test_default_charset_parsing(self):
        """
        Here, we want the database to include the binary charset into
        the DEFAULT_CHARSET optional metadata block.
        Also, we are adding an int field and two text fields to force
        a difference in the index of the blob column in the table
        and in the list of columns that have charset.
        """
        query = """CREATE TABLE test (
            id INT NOT NULL AUTO_INCREMENT,
            text1 VARCHAR(255) NOT NULL,
            text2 VARCHAR(255) NOT NULL,
            data LONGBLOB NOT NULL,
            PRIMARY KEY (id)
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;"""
        self.execute(query)
        query = "INSERT INTO test (text1, text2, data) VALUES(%s, %s, %s)"
        self.execute_with_args(query, ("text", "text", b"data"))
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        # QueryEvent for the Create Table
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        # QueryEvent for the BEGIN
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        if event.table_map[event.table_id].column_name_flag:
            columns = {c.name: c for c in event.columns}
            assert columns["text1"].character_set_name == "utf8"
            assert columns["text1"].collation_name.startswith("utf8")
            assert columns["text2"].character_set_name == "utf8"
            assert columns["text2"].collation_name.startswith("utf8")
            assert columns["data"].character_set_name == "binary"
            assert columns["data"].collation_name == "binary"

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
            ignored_events=self.ignoredEvents(),
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
            only_events=[
                FormatDescriptionEvent,
                QueryEvent,
                TableMapEvent,
                WriteRowsEvent,
                XidEvent,
            ],
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

        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id))"
        )
        self.execute("INSERT INTO test values (NULL)")
        self.execute("INSERT INTO test values (NULL)")
        self.execute("INSERT INTO test values (NULL)")
        self.execute("INSERT INTO test values (NULL)")
        self.execute("INSERT INTO test values (NULL)")
        self.execute("COMMIT")
        # import os
        # os._exit(1)

        binlog = self.execute("SHOW BINARY LOGS").fetchone()[0]

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, log_pos=0, log_file=binlog, end_log_pos=888
        )

        last_log_pos = 0
        last_event_type = 0
        for event in self.stream:
            last_log_pos = self.stream.log_pos
            last_event_type = event.event_type

        self.assertEqual(last_log_pos, 888)
        self.assertEqual(last_event_type, TABLE_MAP_EVENT)

    def test_event_validation(self):
        def create_binlog_packet_wrapper(pkt):
            return BinLogPacketWrapper(
                pkt,
                self.stream.table_map,
                self.stream._ctl_connection,
                self.stream.mysql_version,
                self.stream._BinLogStreamReader__use_checksum,
                self.stream._BinLogStreamReader__allowed_events_in_packet,
                self.stream._BinLogStreamReader__only_tables,
                self.stream._BinLogStreamReader__ignored_tables,
                self.stream._BinLogStreamReader__only_schemas,
                self.stream._BinLogStreamReader__ignored_schemas,
                self.stream._BinLogStreamReader__freeze_schema,
                self.stream._BinLogStreamReader__ignore_decode_errors,
                self.stream._BinLogStreamReader__verify_checksum,
                self.stream._BinLogStreamReader__optional_meta_data,
            )

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, blocking=False, verify_checksum=True
        )
        # For event data, refer to the official document example data of mariaDB.
        # https://mariadb.com/kb/en/query_event/#example-with-crc32
        correct_event_data = (
            # OK value
            b"\x00"
            # Header
            b"q\x17(Z\x02\x8c'\x00\x00U\x00\x00\x00\x01\t\x00\x00\x00\x00"
            # Content
            b"f\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1a\x00"
            b"\x00\x00\x00\x00\x00\x01\x00\x00\x00P\x00\x00"
            b"\x00\x00\x06\x03std\x04\x08\x00\x08\x00\x08\x00\x00"
            b"TRUNCATE TABLE test.t4"
            # CRC 32, 4 Bytes
            b"Ji\x9e\xed"
        )
        # Assume a bit flip occurred while data was being transmitted    q(1001000) -> U(0110111)
        modified_byte = b"U"
        wrong_event_data = (
            correct_event_data[:1] + modified_byte + correct_event_data[2:]
        )

        packet = MysqlPacket(correct_event_data, 0)
        wrong_packet = MysqlPacket(wrong_event_data, 0)
        self.stream.fetchone()  # for '_ctl_connection' parameter
        binlog_event = create_binlog_packet_wrapper(packet)
        wrong_event = create_binlog_packet_wrapper(wrong_packet)
        self.assertEqual(binlog_event.event._is_event_valid, True)
        self.assertNotEqual(wrong_event.event._is_event_valid, True)

    def test_json_update(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, only_events=[UpdateRowsEvent]
        )
        create_query = (
            "CREATE TABLE setting_table( id SERIAL AUTO_INCREMENT, setting JSON);"
        )
        insert_query = """INSERT INTO setting_table (setting) VALUES ('{"btn": true, "model": false}');"""

        update_query = """  UPDATE setting_table
                            SET setting = JSON_REMOVE(setting, '$.model')
                            WHERE id=1;
                        """
        self.execute(create_query)
        self.execute(insert_query)
        self.execute(update_query)
        self.execute("COMMIT;")
        event = self.stream.fetchone()

        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(
                event.rows[0]["before_values"]["setting"],
                {b"btn": True, b"model": False},
            ),
            self.assertEqual(event.rows[0]["after_values"]["setting"], {b"btn": True}),

    def test_format_description_event(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            blocking=False,
            only_events=[FormatDescriptionEvent],
        )

        event = self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        self.assertIsInstance(event.binlog_version, tuple)
        self.assertIsInstance(event.mysql_version_str, str)
        self.assertTrue(
            event.mysql_version_str.startswith("5.")
            or event.mysql_version_str.startswith("8.")
        )  # Example check
        self.assertIsInstance(event.common_header_len, int)
        self.assertIsInstance(event.post_header_len, tuple)
        self.assertIsInstance(event.mysql_version, tuple)
        self.assertEqual(len(event.mysql_version), 3)
        self.assertEqual(event.dbms, "mysql")
        self.assertIsInstance(event.server_version_split, tuple)
        self.assertEqual(len(event.server_version_split), 3)
        self.assertIsInstance(event.number_of_event_types, int)


class TestMultipleRowBinLogStreamReader(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestMultipleRowBinLogStreamReader, self).setUp()
        if self.isMySQL8014AndMore():
            self.execute("SET GLOBAL binlog_row_metadata='FULL';")
            self.execute("SET GLOBAL binlog_row_image='FULL';")

    def ignoredEvents(self):
        return [GtidEvent, PreviousGtidsEvent]

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
        if event.table_map[event.table_id].column_name_flag:
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
        if event.table_map[event.table_id].column_name_flag:
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
        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(event.rows[0]["values"]["id"], 1)
            self.assertEqual(event.rows[0]["values"]["data"], "Hello")

            self.assertEqual(event.rows[1]["values"]["id"], 2)
            self.assertEqual(event.rows[1]["values"]["data"], "World")

    def test_ignore_decode_errors(self):
        if self.isMySQL80AndMore():
            self.skipTest("MYSQL 8 Version Pymysql Data Error Incorrect string value")
        problematic_unicode_string = (
            b'[{"text":"\xed\xa0\xbd \xed\xb1\x8d Some string"}]'
        )
        self.stream.close()
        self.execute("CREATE TABLE test (data VARCHAR(50) CHARACTER SET utf8mb4)")
        self.execute_with_args(
            "INSERT INTO test (data) VALUES (%s)", (problematic_unicode_string)
        )
        self.execute("COMMIT")

        # Initialize with ignore_decode_errors=False
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
            ignore_decode_errors=False,
        )
        with self.assertRaises(UnicodeError):
            event = self.stream.fetchone()
            data = event.rows[0]["values"]["data"]

        # Initialize with ignore_decode_errors=True
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
            ignore_decode_errors=True,
        )
        event = self.stream.fetchone()
        if event.table_map[event.table_id].column_name_flag:
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
            self.database, server_id=1024, only_events=(WriteRowsEvent,)
        )
        try:
            self.stream.fetchone()  # insert with two values
            self.stream.fetchone()  # insert with one value
        except Exception as e:
            self.fail(f"raised unexpected exception: {e}")
        finally:
            self.resetBinLog()

    def test_alter_column(self):
        if not self.isMySQL8014AndMore():
            self.skipTest("Mysql version is under 8.0.14 - pass")
        self.stream.close()
        self.execute(
            "CREATE TABLE test_alter_column (id INTEGER(11), data VARCHAR(50))"
        )
        self.execute("INSERT INTO test_alter_column VALUES (1, 'A value')")
        self.execute("COMMIT")
        self.execute(
            "ALTER TABLE test_alter_column ADD COLUMN another_data VARCHAR(50) AFTER id"
        )
        self.execute(
            "INSERT INTO test_alter_column VALUES (2, 'Another value', 'A value')"
        )
        self.execute("COMMIT")

        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
        )
        event = self.stream.fetchone()
        self.assertEqual(event.rows[0]["values"]["data"], "A value")
        event = self.stream.fetchone()  # insert with three values
        self.assertEqual(event.rows[0]["values"]["another_data"], "Another value")
        self.assertEqual(event.rows[0]["values"]["data"], "A value")


class TestCTLConnectionSettings(base.PyMySQLReplicationTestCase):
    def setUp(self, charset="utf8"):
        super().setUp()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(WriteRowsEvent,),
        )

    def test_separate_ctl_settings_no_error(self):
        self.execute("CREATE TABLE test (id INTEGER(11))")
        self.execute("INSERT INTO test VALUES (1)")
        self.execute("DROP TABLE test")
        self.execute("COMMIT")
        self.conn_control.cursor().execute("CREATE TABLE test (id INTEGER(11))")
        self.conn_control.cursor().execute("INSERT INTO test VALUES (1)")
        self.conn_control.cursor().execute("COMMIT")
        try:
            self.stream.fetchone()
        except Exception as e:
            self.fail(f"raised unexpected exception: {e}")
        finally:
            self.resetBinLog()


class TestGtidBinLogStreamReader(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super().setUp()
        if not self.supportsGTID:
            raise unittest.SkipTest(
                "database does not support GTID, skipping GTID tests"
            )

    def test_read_query_event(self):
        query = "CREATE TABLE test (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "SELECT @@global.gtid_executed;"
        gtid = self.execute(query).fetchone()[0]

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            blocking=True,
            auto_position=gtid,
            ignored_events=[HeartbeatLogEvent],
        )

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        # Insert first event
        query = "BEGIN;"
        self.execute(query)
        query = "INSERT INTO test (id, data) VALUES(1, 'Hello');"
        self.execute(query)
        query = "COMMIT;"
        self.execute(query)

        self.assertIsInstance(self.stream.fetchone(), PreviousGtidsEvent)
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
            self.database,
            server_id=1024,
            blocking=True,
            auto_position=gtid,
            ignored_events=[HeartbeatLogEvent],
        )

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        self.assertIsInstance(self.stream.fetchone(), PreviousGtidsEvent)
        self.assertIsInstance(self.stream.fetchone(), GtidEvent)
        event = self.stream.fetchone()

        self.assertEqual(
            event.query,
            "CREATE TABLE test2 (id INT NOT NULL, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))",
        )


class TestGtidRepresentation(unittest.TestCase):
    def test_gtidset_representation(self):
        set_repr = (
            "57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,"
            "4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20"
        )

        myset = GtidSet(set_repr)
        self.assertEqual(str(myset), set_repr)

    def test_gtidset_representation_newline(self):
        set_repr = (
            "57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,"
            "4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20"
        )
        mysql_repr = (
            "57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,\n"
            "4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20"
        )

        myset = GtidSet(mysql_repr)
        self.assertEqual(str(myset), set_repr)

    def test_gtidset_representation_payload(self):
        set_repr = (
            "57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56,"
            "4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20"
        )

        myset = GtidSet(set_repr)
        payload = myset.encode()
        parsedset = myset.decode(io.BytesIO(payload))

        self.assertEqual(str(myset), str(parsedset))

        set_repr = (
            "57b70f4e-20d3-11e5-a393-4a63946f7eac:1,"
            "4350f323-7565-4e59-8763-4b1b83a0ce0e:1-20"
        )

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
        with self.assertRaises(ValueError):
            Gtid(
                "57b70f4e-20d3-11e5-a393-4a63946f7eac:1-5 57b70f4e-20d3-11e5-a393-4a63946f7eac:1-56"
            )
            Gtid("NNNNNNNN-20d3-11e5-a393-4a63946f7eac:1-5")
            Gtid("-20d3-11e5-a393-4a63946f7eac:1-5")
            Gtid("-20d3-11e5-a393-4a63946f7eac:1-")
            Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:A-1")
            Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:-1")
            Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac:1-:1")
            Gtid("57b70f4e-20d3-11e5-a393-4a63946f7eac::1")


class TestStatementConnectionSetting(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestStatementConnectionSetting, self).setUp()
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(RandEvent, UserVarEvent, QueryEvent),
        )
        self.execute("SET @@binlog_format='STATEMENT'")

    def test_rand_event(self):
        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data INT NOT NULL, PRIMARY KEY (id))"
        )
        self.execute("INSERT INTO test (data) VALUES(RAND())")
        self.execute("COMMIT")

        self.assertEqual(self.bin_log_format(), "STATEMENT")
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        expected_rand_event = self.stream.fetchone()
        self.assertIsInstance(expected_rand_event, RandEvent)
        self.assertEqual(type(expected_rand_event.seed1), int)
        self.assertEqual(type(expected_rand_event.seed2), int)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_user_var_string_event(self, mock_stdout):
        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR(50), PRIMARY KEY (id))"
        )
        self.execute("SET @test_user_var = 'foo'")
        self.execute("INSERT INTO test (data) VALUES(@test_user_var)")
        self.execute("COMMIT")

        self.assertEqual(self.bin_log_format(), "STATEMENT")
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var")
        self.assertEqual(expected_user_var_event.value, "foo")
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 0)
        self.assertEqual(expected_user_var_event.charset, 33)

        # Test _dump method
        expected_user_var_event._dump()
        self.assertIn("User variable name: ", mock_stdout.getvalue())
        self.assertIn("Value: ", mock_stdout.getvalue())

    def test_user_var_real_event(self):
        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data REAL, PRIMARY KEY (id))"
        )
        self.execute("SET @test_user_var = @@timestamp")
        self.execute("INSERT INTO test (data) VALUES(@test_user_var)")
        self.execute("COMMIT")

        self.assertEqual(self.bin_log_format(), "STATEMENT")
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var")
        self.assertIsInstance(expected_user_var_event.value, float)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 1)
        self.assertEqual(expected_user_var_event.charset, 33)

    def test_user_var_int_event(self):
        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data1 INT, data2 INT, data3 INT, PRIMARY KEY (id))"
        )
        self.execute("SET @test_user_var1 = 5")
        self.execute("SET @test_user_var2 = 0")
        self.execute("SET @test_user_var3 = -5")
        self.execute(
            "INSERT INTO test (data1, data2, data3) VALUES(@test_user_var1, @test_user_var2, @test_user_var3)"
        )
        self.execute("COMMIT")

        self.assertEqual(self.bin_log_format(), "STATEMENT")
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var1")
        self.assertEqual(expected_user_var_event.value, 5)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var2")
        self.assertEqual(expected_user_var_event.value, 0)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var3")
        self.assertEqual(expected_user_var_event.value, -5)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

    def test_user_var_int24_event(self):
        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data1 MEDIUMINT, data2 MEDIUMINT, data3 MEDIUMINT UNSIGNED, PRIMARY KEY (id))"
        )
        self.execute("SET @test_user_var1 = 8388607")
        self.execute("SET @test_user_var2 = -8388607")
        self.execute("SET @test_user_var3 = 16777215")
        self.execute(
            "INSERT INTO test (data1, data2, data3) VALUES(@test_user_var1, @test_user_var2, @test_user_var3)"
        )
        self.execute("COMMIT")

        self.assertEqual(self.bin_log_format(), "STATEMENT")
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var1")
        self.assertEqual(expected_user_var_event.value, 8388607)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var2")
        self.assertEqual(expected_user_var_event.value, -8388607)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var3")
        self.assertEqual(expected_user_var_event.value, 16777215)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

    def test_user_var_longlong_event(self):
        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data1 BIGINT, data2 BIGINT, data3 BIGINT UNSIGNED, PRIMARY KEY (id))"
        )
        self.execute("SET @test_user_var1 = 9223372036854775807")
        self.execute("SET @test_user_var2 = -9223372036854775808")
        self.execute("SET @test_user_var3 = 18446744073709551615")
        self.execute(
            "INSERT INTO test (data1, data2, data3) VALUES(@test_user_var1, @test_user_var2, @test_user_var3)"
        )
        self.execute("COMMIT")

        self.assertEqual(self.bin_log_format(), "STATEMENT")
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var1")
        self.assertEqual(expected_user_var_event.value, 9223372036854775807)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var2")
        self.assertEqual(expected_user_var_event.value, -9223372036854775808)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var3")
        self.assertEqual(expected_user_var_event.value, 18446744073709551615)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 2)
        self.assertEqual(expected_user_var_event.charset, 33)

    def test_user_var_decimal_event(self):
        self.execute(
            "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data1 DECIMAL, data2 DECIMAL, PRIMARY KEY (id))"
        )
        self.execute("SET @test_user_var1 = 5.25")
        self.execute("SET @test_user_var2 = -5.25")
        self.execute(
            "INSERT INTO test (data1, data2) VALUES(@test_user_var1, @test_user_var2)"
        )
        self.execute("COMMIT")

        self.assertEqual(self.bin_log_format(), "STATEMENT")
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var1")
        self.assertEqual(expected_user_var_event.value, 5.25)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 4)
        self.assertEqual(expected_user_var_event.charset, 33)

        expected_user_var_event = self.stream.fetchone()
        self.assertIsInstance(expected_user_var_event, UserVarEvent)
        self.assertIsInstance(expected_user_var_event.name_len, int)
        self.assertEqual(expected_user_var_event.name, "test_user_var2")
        self.assertEqual(expected_user_var_event.value, -5.25)
        self.assertEqual(expected_user_var_event.is_null, 0)
        self.assertEqual(expected_user_var_event.type, 4)
        self.assertEqual(expected_user_var_event.charset, 33)

    def tearDown(self):
        self.execute("SET @@binlog_format='ROW'")
        self.assertEqual(self.bin_log_format(), "ROW")
        super(TestStatementConnectionSetting, self).tearDown()


@pytest.mark.mariadb
class TestMariadbBinlogStreamReader(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super().setUp()
        if not self.isMariaDB():
            self.skipTest("Skipping the entire class for MariaDB")

    def test_binlog_checkpoint_event(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database, server_id=1023, blocking=False, is_mariadb=True
        )

        query = "DROP TABLE IF EXISTS test"
        self.execute(query)

        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        self.stream.close()

        event = self.stream.fetchone()
        self.assertIsInstance(event, RotateEvent)

        event = self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)

        event = self.stream.fetchone()
        self.assertIsInstance(event, MariadbStartEncryptionEvent)

        event = self.stream.fetchone()
        self.assertIsInstance(event, MariadbGtidListEvent)

        event = self.stream.fetchone()
        self.assertIsInstance(event, MariadbBinLogCheckPointEvent)
        self.assertEqual(event.filename, self.bin_log_basename() + ".000001")


@pytest.mark.mariadb
class TestMariadbBinlogStreamReader2(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super().setUp()
        if not self.isMariaDB():
            self.skipTest("Skipping the entire class for MariaDB")

    def test_annotate_rows_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        # Insert first event
        query = "BEGIN;"
        self.execute(query)
        insert_query = b"INSERT INTO test (id, data) VALUES(1, 'Hello')"
        self.execute(insert_query)
        query = "COMMIT;"
        self.execute(query)

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            blocking=False,
            only_events=[MariadbAnnotateRowsEvent],
            is_mariadb=True,
            annotate_rows_event=True,
        )

        event = self.stream.fetchone()
        # Check event type 160,MariadbAnnotateRowsEvent
        self.assertEqual(event.event_type, 160)
        # Check self.sql_statement
        self.assertEqual(event.sql_statement, insert_query)
        self.assertIsInstance(event, MariadbAnnotateRowsEvent)

    def test_start_encryption_event(self):
        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"
        self.execute(query)
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)

        start_encryption_event = self.stream.fetchone()
        self.assertIsInstance(start_encryption_event, MariadbStartEncryptionEvent)

        schema = start_encryption_event.schema
        key_version = start_encryption_event.key_version
        nonce = start_encryption_event.nonce

        from pathlib import Path

        encryption_key_file_path = Path(__file__).parent.parent.parent

        try:
            with open(
                f"{encryption_key_file_path}/.mariadb/no_encryption_key.key", "r"
            ) as key_file:
                first_line = key_file.readline()
                key_version_from_key_file = int(first_line.split(";")[0])
        except Exception as e:
            self.fail(f"raised unexpected exception: {e}")
        finally:
            self.resetBinLog()

        # schema is always 1
        self.assertEqual(schema, 1)
        self.assertEqual(key_version, key_version_from_key_file)
        self.assertEqual(type(nonce), bytes)
        self.assertEqual(len(nonce), 12)

    def test_gtid_list_event(self):
        # set max_binlog_size to create new binlog file
        query = "SET GLOBAL max_binlog_size=4096"
        self.execute(query)
        # parse only Maradb GTID list event
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            blocking=False,
            only_events=[MariadbGtidListEvent],
            is_mariadb=True,
        )

        query = "CREATE TABLE test (id INT NOT NULL AUTO_INCREMENT, data VARCHAR (50) NOT NULL, PRIMARY KEY (id))"
        self.execute(query)
        query = "INSERT INTO test (data) VALUES('Hello World')"

        for cnt in range(0, 15):
            self.execute(query)
            self.execute("COMMIT")

        # 'mariadb gtid list event' of first binlog file
        event = self.stream.fetchone()
        self.assertEqual(event.event_type, 163)
        self.assertIsInstance(event, MariadbGtidListEvent)

        # 'mariadb gtid list event' of second binlog file
        event = self.stream.fetchone()
        self.assertEqual(event.event_type, 163)
        self.assertEqual(event.gtid_list[0].gtid, "0-1-15")

    def test_format_description_event(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            blocking=False,
            only_events=[FormatDescriptionEvent],
            is_mariadb=True,
        )

        event = self.stream.fetchone()
        self.assertIsInstance(event, FormatDescriptionEvent)
        self.assertIsInstance(event.binlog_version, tuple)
        self.assertIsInstance(event.mysql_version_str, str)
        self.assertTrue(event.mysql_version_str.startswith("10."))
        self.assertIsInstance(event.common_header_len, int)
        self.assertIsInstance(event.post_header_len, tuple)
        self.assertIsInstance(event.mysql_version, tuple)
        self.assertEqual(len(event.mysql_version), 3)
        self.assertEqual(event.dbms, "mariadb")
        self.assertIsInstance(event.server_version_split, tuple)
        self.assertEqual(len(event.server_version_split), 3)
        self.assertIsInstance(event.number_of_event_types, int)


class TestRowsQueryLogEvents(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestRowsQueryLogEvents, self).setUp()
        self.execute("SET SESSION binlog_rows_query_log_events=1")

    def tearDown(self):
        self.execute("SET SESSION binlog_rows_query_log_events=0")
        super(TestRowsQueryLogEvents, self).tearDown()

    def test_rows_query_log_event(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=[RowsQueryLogEvent],
        )
        self.execute(
            "CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))"
        )
        self.execute("INSERT INTO test (name) VALUES ('Soul Lee')")
        self.execute("COMMIT")
        event = self.stream.fetchone()
        self.assertIsInstance(event, RowsQueryLogEvent)

    def test_long_query(self):
        """
        Address issue #601
        Do not use the first byte of the body to determine the length of the query.
        1 byte can not represent the length of a query that is longer than 255 bytes.
        """

        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=[RowsQueryLogEvent],
        )

        self.execute(
            "CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, long_text VARCHAR(256))"
        )
        long_query = (
            "INSERT INTO test (long_text) VALUES ('"
            "What is the longest word in english?"
            "Pneumonoultramicroscopicsilicovolcanoconiosis is the longest word in the English language."
            "This text has 256 characters and hence its length can not be represented in a single byte."
            "')"
        )
        self.execute(long_query)
        self.execute("COMMIT")
        event = self.stream.fetchone()
        self.assertIsInstance(event, RowsQueryLogEvent)
        self.assertEqual(event.query, long_query)


class TestGtidEvent(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestGtidEvent, self).setUp()
        self.execute("SET SESSION binlog_rows_query_log_events=1")

    def tearDown(self):
        self.execute("SET SESSION binlog_rows_query_log_events=0")
        super(TestGtidEvent, self).tearDown()

    def test_gtid_event(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=[GtidEvent, FormatDescriptionEvent],
        )
        if not self.isMySQL801AndMore():
            self.skipTest("Mysql version is under 8.0.1")
        self.execute(
            "CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))"
        )
        format_description_event = self.stream.fetchone()
        gtid_event = self.stream.fetchone()
        self.assertIsInstance(format_description_event, FormatDescriptionEvent)
        self.assertIsInstance(gtid_event, GtidEvent)
        self.assertIsInstance(gtid_event.event_type, int)
        self.assertIsInstance(gtid_event.sid, bytes)
        self.assertIsInstance(gtid_event.gno, int)
        self.assertIsInstance(gtid_event.lt_type, int)
        self.assertIsInstance(gtid_event.last_committed, int)
        self.assertIsInstance(gtid_event.sequence_number, int)
        self.assertEqual(gtid_event.sequence_number, 1)
        self.assertIsInstance(gtid_event.immediate_commit_timestamp, bytes)
        self.assertIsInstance(gtid_event.original_commit_timestamp, bytes)


class TestLatin1(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super().setUp(charset="latin1")

    def test_query_event_latin1(self):
        """
        Ensure query events with a non-utf8 encoded query are parsed without errors.
        """
        self.stream = BinLogStreamReader(
            self.database, server_id=1024, only_events=[QueryEvent]
        )
        self.execute("CREATE TABLE test_latin1_ÖÆÛ (a INT)")
        self.execute("COMMIT")
        assert "ÖÆÛ".encode("latin-1") == b"\xd6\xc6\xdb"

        event = self.stream.fetchone()
        assert event.query.startswith("CREATE TABLE test")
        assert event.query == r"CREATE TABLE test_latin1_\xd6\xc6\xdb (a INT)"


@pytest.mark.mariadb
class TestOptionalMetaData(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestOptionalMetaData, self).setUp()
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(TableMapEvent,),
        )
        if not self.isMySQL8014AndMore():
            self.skipTest("Mysql version is under 8.0.14 - pass TestOptionalMetaData")
        self.execute("SET GLOBAL binlog_row_metadata='FULL';")

    def test_signedness(self):
        create_query = "CREATE TABLE test_signedness (col1 INT, col2 INT UNSIGNED);"
        insert_query = "INSERT INTO test_signedness VALUES (-10, 10);"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(event.optional_metadata.unsigned_column_list, [False, True])

    def test_default_charset(self):
        create_query = "CREATE TABLE test_default_charset (name VARCHAR(50)) CHARACTER SET utf8mb4;"
        insert_query = "INSERT INTO test_default_charset VALUES ('Hello, World!');"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        if self.isMariaDB():
            self.assertEqual(event.optional_metadata.default_charset_collation, 45)
        else:
            self.assertEqual(event.optional_metadata.default_charset_collation, 255)

    def test_column_charset(self):
        create_query = "CREATE TABLE test_column_charset (col1 VARCHAR(50), col2 VARCHAR(50) CHARACTER SET binary, col3 VARCHAR(50) CHARACTER SET latin1);"
        insert_query = (
            "INSERT INTO test_column_charset VALUES ('python', 'mysql', 'replication');"
        )

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        if self.isMariaDB():
            self.assertEqual(event.optional_metadata.column_charset, [45, 63, 8])
        else:
            self.assertEqual(event.optional_metadata.column_charset, [255, 63, 8])

    def test_column_name(self):
        create_query = "CREATE TABLE test_column_name (col_int INT, col_varchar VARCHAR(30), col_bool BOOL);"
        insert_query = "INSERT INTO test_column_name VALUES (1, 'Hello', true);"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(
            event.optional_metadata.column_name_list,
            ["col_int", "col_varchar", "col_bool"],
        )

    def test_set_str_value(self):
        create_query = "CREATE TABLE test_set_str_value (skills SET('Programming', 'Writing', 'Design'));"
        insert_query = "INSERT INTO test_set_str_value VALUES ('Programming,Writing');"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(
            event.optional_metadata.set_str_value_list,
            [["Programming", "Writing", "Design"]],
        )

    def test_enum_str_value(self):
        create_query = "CREATE TABLE test_enum_str_value (pet ENUM('Dog', 'Cat'));"
        insert_query = "INSERT INTO test_enum_str_value VALUES ('Cat');"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(
            event.optional_metadata.set_enum_str_value_list, [["Dog", "Cat"]]
        )

    def test_geometry_type(self):
        create_query = "CREATE TABLE test_geometry_type (location POINT);"
        insert_query = "INSERT INTO test_geometry_type VALUES (Point(37.123, 125.987));"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(event.optional_metadata.geometry_type_list, [1])

    def test_simple_primary_key(self):
        create_query = "CREATE TABLE test_simple_primary_key (c_key1 INT, c_key2 INT, c_not_key INT, PRIMARY KEY(c_key1, c_key2));"
        insert_query = "INSERT INTO test_simple_primary_key VALUES (1, 2, 3);"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(event.optional_metadata.simple_primary_key_list, [0, 1])

    def test_primary_key_with_prefix(self):
        create_query = "CREATE TABLE test_primary_key_with_prefix (c_key1 CHAR(100), c_key2 CHAR(10), c_not_key INT, c_key3 CHAR(100), PRIMARY KEY(c_key1(5), c_key2, c_key3(10)));"
        insert_query = (
            "INSERT INTO test_primary_key_with_prefix VALUES('1', '2', 3, '4');"
        )

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(
            event.optional_metadata.primary_keys_with_prefix, {0: 5, 1: 0, 3: 10}
        )

    def test_enum_and_set_default_charset(self):
        create_query = "CREATE TABLE test_enum_and_set_default_charset (pet ENUM('Dog', 'Cat'), skills SET('Programming', 'Writing', 'Design')) CHARACTER SET utf8mb4;"
        insert_query = (
            "INSERT INTO test_enum_and_set_default_charset VALUES('Dog', 'Design');"
        )

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        if self.isMariaDB():
            self.assertEqual(
                event.optional_metadata.enum_and_set_collation_list, [45, 45]
            )
        else:
            self.assertEqual(
                event.optional_metadata.enum_and_set_collation_list, [255, 255]
            )

    def test_enum_and_set_column_charset(self):
        create_query = "CREATE TABLE test_enum_and_set_column_charset (pet ENUM('Dog', 'Cat') CHARACTER SET utf8mb4, number SET('00', '01', '10', '11') CHARACTER SET binary);"
        insert_query = (
            "INSERT INTO test_enum_and_set_column_charset VALUES('Cat', '10');"
        )

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        if self.isMariaDB():
            self.assertEqual(
                event.optional_metadata.enum_and_set_collation_list, [45, 63]
            )
        else:
            self.assertEqual(
                event.optional_metadata.enum_and_set_collation_list, [255, 63]
            )

    def test_visibility(self):
        mysql_version = self.getMySQLVersion()
        version = float(mysql_version.rsplit(".", 1)[0])
        version_detail = int(mysql_version.rsplit(".", 1)[1])
        if not (version >= 8.0 and version_detail >= 23):
            self.skipTest("Mysql version  8.0.23 - visibility supprot")
        create_query = "CREATE TABLE test_visibility (name VARCHAR(50), secret_key VARCHAR(50) DEFAULT 'qwerty' INVISIBLE);"
        insert_query = "INSERT INTO test_visibility VALUES('Audrey');"

        self.execute(create_query)
        self.execute(insert_query)
        self.execute("COMMIT")

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        if not self.isMariaDB():
            self.assertEqual(event.optional_metadata.visibility_list, [True, False])

    def test_sync_drop_table_map_event_table_schema(self):
        create_query = "CREATE TABLE test_sync (name VARCHAR(50) comment 'test_sync');"
        insert_query = "INSERT INTO test_sync VALUES('Audrey');"
        self.execute(create_query)
        self.execute(insert_query)

        self.execute("COMMIT")
        drop_query = "DROP TABLE test_sync;"
        self.execute(drop_query)
        select_query = """
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION,
                        DATA_TYPE, CHARACTER_OCTET_LENGTH
                    FROM
                        information_schema.columns
                    WHERE
                        table_name = "test_sync"
                    ORDER BY ORDINAL_POSITION
                    """
        column_schemas = self.execute(select_query).fetchall()

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(event.table_obj.data["columns"][0].name, "name")
        self.assertEqual(len(column_schemas), 0)

    def test_sync_column_drop_event_table_schema(self):
        create_query = "CREATE TABLE test_sync (drop_column1 VARCHAR(50) , drop_column2 VARCHAR(50) , drop_column3 VARCHAR(50));"
        insert_query = "INSERT INTO test_sync VALUES('Audrey','Sean','Test');"
        self.execute(create_query)
        self.execute(insert_query)

        self.execute("COMMIT")
        alter_query = "ALTER TABLE test_sync DROP drop_column2;"
        self.execute(alter_query)
        select_query = """
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION,
                        DATA_TYPE, CHARACTER_OCTET_LENGTH
                    FROM
                        information_schema.columns
                    WHERE
                        table_name = "test_sync"
                    ORDER BY ORDINAL_POSITION
                    """
        column_schemas = self.execute(select_query).fetchall()

        event = self.stream.fetchone()
        self.assertIsInstance(event, TableMapEvent)
        self.assertEqual(len(column_schemas), 2)
        self.assertEqual(len(event.table_obj.data["columns"]), 3)
        self.assertEqual(column_schemas[0][0], "drop_column1")
        self.assertEqual(column_schemas[1][0], "drop_column3")
        self.assertEqual(event.table_obj.data["columns"][0].name, "drop_column1")
        self.assertEqual(event.table_obj.data["columns"][1].name, "drop_column2")
        self.assertEqual(event.table_obj.data["columns"][2].name, "drop_column3")

    def tearDown(self):
        self.execute("SET GLOBAL binlog_row_metadata='MINIMAL';")
        super(TestOptionalMetaData, self).tearDown()


class TestColumnValueNoneSources(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestColumnValueNoneSources, self).setUp()
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(TableMapEvent,),
        )
        if not self.isMySQL8014AndMore():
            self.skipTest(
                "Mysql version is under 8.0.14 - pass TestColumnValueNoneSources"
            )
        self.execute("SET GLOBAL binlog_row_metadata='FULL';")

    def test_get_none(self):
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            resume_stream=False,
            only_events=[WriteRowsEvent],
        )
        query = "CREATE TABLE null_operation_update_example (col1 INT, col2 INT);"
        self.execute(query)
        query = (
            "INSERT INTO null_operation_update_example (col1, col2) VALUES (NULL, 1);"
        )
        self.execute(query)
        self.execute("COMMIT")
        write_rows_event = self.stream.fetchone()
        self.assertIsInstance(write_rows_event, WriteRowsEvent)

        none_sources = write_rows_event.rows[0].get("none_sources")
        if none_sources:
            self.assertEqual(none_sources["col1"], NULL)

    def test_get_none_invalid(self):
        self.execute("SET SESSION SQL_MODE='ALLOW_INVALID_DATES'")
        self.execute(
            "CREATE TABLE test_table (col0 INT, col1 VARCHAR(10), col2 DATETIME, col3 DATE, col4 SET('a', 'b', 'c'))"
        )
        self.execute(
            "INSERT INTO test_table VALUES (NULL, NULL, '0000-00-00 00:00:00', NULL, NULL)"
        )
        self.resetBinLog()
        self.execute(
            "UPDATE test_table SET col1 = NULL, col2 = NULL, col3='0000-00-00', col4='d' WHERE col0 IS NULL"
        )
        self.execute("COMMIT")

        self.assertIsInstance(self.stream.fetchone(), RotateEvent)
        self.assertIsInstance(self.stream.fetchone(), FormatDescriptionEvent)
        self.assertIsInstance(self.stream.fetchone(), PreviousGtidsEvent)
        self.assertIsInstance(self.stream.fetchone(), GtidEvent)
        self.assertIsInstance(self.stream.fetchone(), QueryEvent)
        self.assertIsInstance(self.stream.fetchone(), TableMapEvent)

        event = self.stream.fetchone()
        if self.isMySQL56AndMore():
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V2)
        else:
            self.assertEqual(event.event_type, UPDATE_ROWS_EVENT_V1)
        self.assertIsInstance(event, UpdateRowsEvent)

        before_none_sources = event.rows[0].get("before_none_sources")
        after_none_sources = event.rows[0].get("after_none_sources")

        if before_none_sources:
            self.assertEqual(before_none_sources["col0"], NULL)
            self.assertEqual(before_none_sources["col1"], NULL)
            self.assertEqual(before_none_sources["col2"], OUT_OF_DATETIME2_RANGE)
            self.assertEqual(before_none_sources["col3"], NULL)
            self.assertEqual(before_none_sources["col4"], NULL)

        if after_none_sources:
            self.assertEqual(after_none_sources["col0"], NULL)
            self.assertEqual(after_none_sources["col1"], NULL)
            self.assertEqual(after_none_sources["col2"], NULL)
            self.assertEqual(after_none_sources["col3"], OUT_OF_DATE_RANGE)
            self.assertEqual(after_none_sources["col4"], EMPTY_SET)


class TestJsonPartialUpdate(base.PyMySQLReplicationTestCase):
    def setUp(self):
        super(TestJsonPartialUpdate, self).setUp()
        self.stream.close()
        self.stream = BinLogStreamReader(
            self.database,
            server_id=1024,
            only_events=(PartialUpdateRowsEvent,),
        )
        if not self.isMySQL8014AndMore():
            self.skipTest("Mysql version is under 8.0.14 - pass TestJsonPartialUpdate")
        self.execute("SET SESSION binlog_row_image = 'FULL';")
        self.execute("SET SESSION binlog_row_value_options = 'PARTIAL_JSON';")

    def test_json_partial_update(self):
        create_query = "CREATE TABLE test_json_v2 (id INT, c JSON,PRIMARY KEY (id)) ;"
        column_add_query = "ALTER TABLE test_json_v2 ADD COLUMN d JSON DEFAULT NULL, ADD COLUMN e JSON DEFAULT NULL;"
        insert_query = """INSERT INTO test_json_v2 VALUES
                            (101
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}');"""
        update_query = """UPDATE test_json_v2 SET c = JSON_SET(c, '$.ab', '["ab_updatedccc"]') WHERE id = 101;"""

        self.execute(create_query)
        self.execute(column_add_query)
        self.execute(insert_query)
        self.execute(update_query)

        self.execute("COMMIT;")
        event = self.stream.fetchone()

        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(
                event.rows[0]["before_values"]["c"],
                {
                    b"a": b"aaaaaaaaaaaaa",
                    b"c": b"ccccccccccccccc",
                    b"ab": [b"abababababababa", b"babababababab"],
                },
            ),
            after_value_c: JsonDiff = event.rows[0]["after_values"]["c"]
            self.assertEqual(after_value_c.op, JsonDiffOperation.Replace)
            self.assertEqual(after_value_c.path, b"$.ab")
            self.assertEqual(after_value_c.value, b'["ab_updatedccc"]')

            after_none_sources = event.rows[0].get("after_none_sources")
            self.assertEqual(after_none_sources["d"], NONE_SOURCE.JSON_PARTIAL_UPDATE)
            self.assertEqual(after_none_sources["e"], NONE_SOURCE.JSON_PARTIAL_UPDATE)

    def test_json_partial_update_column_value_none(self):
        drop_table_if_exists_query = "DROP TABLE IF EXISTS test_json_v2;"
        create_query = "CREATE TABLE test_json_v2 (id INT, c JSON,PRIMARY KEY (id)) ;"
        column_add_query = "ALTER TABLE test_json_v2 ADD COLUMN d JSON DEFAULT NULL, ADD COLUMN e JSON DEFAULT NULL;"
        insert_query = """INSERT INTO test_json_v2 VALUES
                            (101
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}');"""
        update_query = """UPDATE test_json_v2 SET e = JSON_SET(e, '$.ab', '["ab_updatedeee"]'), c=NULL WHERE id = 101;"""

        self.execute(drop_table_if_exists_query)
        self.execute(create_query)
        self.execute(column_add_query)
        self.execute(insert_query)
        self.execute(update_query)

        self.execute("COMMIT;")
        event = self.stream.fetchone()

        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(
                event.rows[0]["before_values"]["e"],
                {
                    b"a": b"aaaaaaaaaaaaa",
                    b"c": b"ccccccccccccccc",
                    b"ab": [b"abababababababa", b"babababababab"],
                },
            ),
            after_value_e: JsonDiff = event.rows[0]["after_values"]["e"]
            self.assertEqual(after_value_e.op, JsonDiffOperation.Replace)
            self.assertEqual(after_value_e.path, b"$.ab")
            self.assertEqual(after_value_e.value, b'["ab_updatedeee"]')

            after_none_sources = event.rows[0].get("after_none_sources")
            self.assertEqual(after_none_sources["c"], NONE_SOURCE.NULL)
            self.assertEqual(after_none_sources["d"], NONE_SOURCE.JSON_PARTIAL_UPDATE)

    def test_json_partial_update_json_remove(self):
        drop_table_if_exists_query = "DROP TABLE IF EXISTS test_json_v2;"
        create_query = "CREATE TABLE test_json_v2 (id INT, c JSON,PRIMARY KEY (id)) ;"
        column_add_query = "ALTER TABLE test_json_v2 ADD COLUMN d JSON DEFAULT NULL, ADD COLUMN e JSON DEFAULT NULL;"
        insert_query = """INSERT INTO test_json_v2 VALUES
                            (101
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}');"""
        update_query = (
            """UPDATE test_json_v2 SET e = JSON_REMOVE(e, '$.ab') WHERE id = 101;"""
        )

        self.execute(drop_table_if_exists_query)
        self.execute(create_query)
        self.execute(column_add_query)
        self.execute(insert_query)
        self.execute(update_query)

        self.execute("COMMIT;")
        event = self.stream.fetchone()

        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(
                event.rows[0]["before_values"]["e"],
                {
                    b"a": b"aaaaaaaaaaaaa",
                    b"c": b"ccccccccccccccc",
                    b"ab": [b"abababababababa", b"babababababab"],
                },
            ),
            after_value_e: JsonDiff = event.rows[0]["after_values"]["e"]
            self.assertEqual(after_value_e.op, JsonDiffOperation.Remove)
            self.assertEqual(after_value_e.path, b"$.ab")
            self.assertEqual(after_value_e.value, None)

            after_none_sources = event.rows[0].get("after_none_sources")
            self.assertEqual(after_none_sources["c"], NONE_SOURCE.JSON_PARTIAL_UPDATE)
            self.assertEqual(after_none_sources["d"], NONE_SOURCE.JSON_PARTIAL_UPDATE)

    def test_json_partial_update_two_column(self):
        drop_table_if_exists_query = "DROP TABLE IF EXISTS test_json_v2;"
        create_query = "CREATE TABLE test_json_v2 (id INT, c JSON,PRIMARY KEY (id)) ;"
        column_add_query = "ALTER TABLE test_json_v2 ADD COLUMN d JSON DEFAULT NULL, ADD COLUMN e JSON DEFAULT NULL;"
        insert_query = """INSERT INTO test_json_v2 VALUES
                            (101
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'
                            ,'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}');"""
        update_query = """UPDATE test_json_v2 SET d = JSON_SET(d, '$.ab', '["ab_ddd"]'), e = JSON_REMOVE(e, '$.ab') WHERE id = 101;"""

        self.execute(drop_table_if_exists_query)
        self.execute(create_query)
        self.execute(column_add_query)
        self.execute(insert_query)
        self.execute(update_query)

        self.execute("COMMIT;")
        event = self.stream.fetchone()

        if event.table_map[event.table_id].column_name_flag:
            self.assertEqual(
                event.rows[0]["before_values"]["d"],
                {
                    b"a": b"aaaaaaaaaaaaa",
                    b"c": b"ccccccccccccccc",
                    b"ab": [b"abababababababa", b"babababababab"],
                },
            ),
            self.assertEqual(
                event.rows[0]["before_values"]["e"],
                {
                    b"a": b"aaaaaaaaaaaaa",
                    b"c": b"ccccccccccccccc",
                    b"ab": [b"abababababababa", b"babababababab"],
                },
            ),
            after_value_d: JsonDiff = event.rows[0]["after_values"]["d"]
            self.assertEqual(after_value_d.op, JsonDiffOperation.Replace)
            self.assertEqual(after_value_d.path, b"$.ab")
            self.assertEqual(after_value_d.value, b'["ab_ddd"]')

            after_value_e: JsonDiff = event.rows[0]["after_values"]["e"]
            self.assertEqual(after_value_e.op, JsonDiffOperation.Remove)
            self.assertEqual(after_value_e.path, b"$.ab")
            self.assertEqual(after_value_e.value, None)

            after_none_sources = event.rows[0].get("after_none_sources")
            self.assertEqual(after_none_sources["c"], NONE_SOURCE.JSON_PARTIAL_UPDATE)


if __name__ == "__main__":
    import unittest

    unittest.main()
