import binascii
import struct
import datetime
import decimal
import zlib
import logging

from pymysqlreplication.constants.STATUS_VAR_KEY import *
from pymysqlreplication.exceptions import StatusVariableMismatch
from pymysqlreplication.util.bytes import parse_decimal_from_bytes
from typing import Union, Optional
import json


class BinLogEvent(object):
    def __init__(
        self,
        from_packet,
        event_size,
        table_map,
        ctl_connection,
        mysql_version=(0, 0, 0),
        only_tables=None,
        ignored_tables=None,
        only_schemas=None,
        ignored_schemas=None,
        freeze_schema=False,
        ignore_decode_errors=False,
        verify_checksum=False,
        optional_meta_data=False,
    ):
        self.packet = from_packet
        self.table_map = table_map
        self.event_type = self.packet.event_type
        self.timestamp = self.packet.timestamp
        self.event_size = event_size
        self._ctl_connection = ctl_connection
        self.mysql_version = mysql_version
        self._ignore_decode_errors = ignore_decode_errors
        self._verify_checksum = verify_checksum
        self._is_event_valid = None
        # The event have been fully processed, if processed is false
        # the event will be skipped
        self._processed = True
        self.complete = True
        self._verify_event()
        self.dbms = self._ctl_connection._get_dbms()

    def _read_table_id(self):
        # Table ID is 6 byte
        # pad little-endian number
        table_id = self.packet.read(6) + b"\x00\x00"
        return struct.unpack("<Q", table_id)[0]

    def _verify_event(self):
        if not self._verify_checksum:
            return

        self.packet.rewind(1)
        data = self.packet.read(19 + self.event_size)
        footer = self.packet.read(4)
        byte_data = zlib.crc32(data).to_bytes(4, byteorder="little")
        self._is_event_valid = True if byte_data == footer else False
        if not self._is_event_valid:
            logging.error(
                f"An CRC32 has failed for the event type {self.event_type}, "
                "indicating a potential integrity issue with the data."
            )
        self.packet.read_bytes -= 19 + self.event_size + 4
        self.packet.rewind(20)

    @property
    def formatted_timestamp(self) -> str:
        return datetime.datetime.utcfromtimestamp(self.timestamp).isoformat()

    def dump(self):
        print(f"=== {self.__class__.__name__} ===")
        print(f"Date: {self.formatted_timestamp}")
        print(f"Log position: {self.packet.log_pos}")
        print(f"Event size: {self.event_size}")
        print(f"Read bytes: {self.packet.read_bytes}")
        self._dump()
        print()

    def to_dict(self) -> dict:
        return {
            "timestamp": self.formatted_timestamp,
            "log_pos": self.packet.log_pos,
            "event_size": self.event_size,
            "read_bytes": self.packet.read_bytes,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def _dump(self):
        """Core data dumped for the event"""
        pass


class GtidEvent(BinLogEvent):
    """
    GTID change in binlog event

    For more information: `[GTID] <https://mariadb.com/kb/en/gtid/>`_ `[see also] <https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Gtid__event.html>`_

    :ivar commit_flag: 1byte - 00000001 = Transaction may have changes logged with SBR.
            In 5.6, 5.7.0-5.7.18, and 8.0.0-8.0.1, this flag is always set. Starting in 5.7.19 and 8.0.2, this flag is cleared if the transaction only contains row events. It is set if any part of the transaction is written in statement format.
    :ivar sid: 16 byte sequence - UUID representing the SID
    :ivar gno: int - Group number, second component of GTID.
    :ivar lt_type: int(1 byte) - The type of logical timestamp used in the logical clock fields.
    :ivar last_committed: Store the transaction's commit parent sequence_number
    :ivar sequence_number: The transaction's logical timestamp assigned at prepare phase
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        self.commit_flag = struct.unpack("!B", self.packet.read(1))[0] == 1
        self.sid = self.packet.read(16)
        self.gno = struct.unpack("<Q", self.packet.read(8))[0]
        self.lt_type = self.packet.read(1)[0]

        if self.mysql_version >= (5, 7):
            self.last_committed = struct.unpack("<Q", self.packet.read(8))[0]
            self.sequence_number = struct.unpack("<Q", self.packet.read(8))[0]

        format_string = "<Q"
        read_immediate_commit_timestamp = self.packet.read(7)
        self.immediate_commit_timestamp = read_immediate_commit_timestamp.ljust(
            struct.calcsize(format_string), b"\x00"
        )
        self.immediate_commit_timestamp = struct.unpack(
            format_string, self.immediate_commit_timestamp
        )[0]
        self.immediate_commit_timestamp = self.immediate_commit_timestamp & (
            (1 << (8 * struct.calcsize(format_string))) - 1
        )
        self.immediate_commit_timestamp = struct.pack(
            format_string, self.immediate_commit_timestamp
        )

        read_original_commit_timestamp = self.packet.read(7)
        self.original_commit_timestamp = read_original_commit_timestamp.ljust(
            struct.calcsize(format_string), b"\x00"
        )
        self.original_commit_timestamp = struct.unpack(
            format_string, self.original_commit_timestamp
        )[0]
        self.original_commit_timestamp = self.original_commit_timestamp & (
            (1 << (8 * struct.calcsize(format_string))) - 1
        )
        self.original_commit_timestamp = struct.pack(
            format_string, self.original_commit_timestamp
        )

        # todo(hun): We should implement the properties below,
        #            but due to the lack of documentation on the variable-length property,
        #            we've left it as future work.
        # - transaction_length, unknown Variable Length
        # - immediate_server_version, 4 bytes
        # - original_server_version, 4 bytes
        # - Commit group ticket, 8 bytes, This property is only introduced in the CPP implementation.

    @property
    def gtid(self):
        """
        GTID = source_id:transaction_id
        Eg: 3E11FA47-71CA-11E1-9E33-C80AA9429562:23
        See: http://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html"""
        nibbles = binascii.hexlify(self.sid).decode("ascii")
        gtid = (
            f"{nibbles[:8]}-"
            f"{nibbles[8:12]}-"
            f"{nibbles[12:16]}-"
            f"{nibbles[16:20]}-"
            f"{nibbles[20:]}:"
            f"{self.gno}"
        )
        return gtid

    def _dump(self):
        print(f"Commit: {self.commit_flag}")
        print(f"GTID_NEXT: {self.gtid}")
        if hasattr(self, "last_committed"):
            print(f"last_committed: {self.last_committed}")
            print(f"sequence_number: {self.sequence_number}")

    def __repr__(self):
        return f'<GtidEvent "{self.gtid}">'


class PreviousGtidsEvent(BinLogEvent):
    """
    PreviousGtidEvent is contains the Gtids executed in the last binary log file.
    Attributes:
        n_sid: which size is the gtid_set
        sid: 16bytes UUID as a binary
        n_intervals: how many intervals are sent
    Eg: [4c9e3dfc-9d25-11e9-8d2e-0242ac1cfd7e:1-100, 4c9e3dfc-9d25-11e9-8d2e-0242ac1cfd7e:1-10:20-30]
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(PreviousGtidsEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        self._n_sid = self.packet.read_int64()
        self._gtids = []

        for _ in range(self._n_sid):
            sid = self.packet.read(16)
            n_intervals = self.packet.read_uint64()
            intervals = [
                f"{self.packet.read_int64()}-{self.packet.read_uint64()}"
                for _ in range(n_intervals)
            ]
            nibbles = binascii.hexlify(sid).decode("ascii")
            gtid = (
                f"{nibbles[:8]}-"
                f"{nibbles[8:12]}-"
                f"{nibbles[12:16]}-"
                f"{nibbles[16:20]}-"
                f"{nibbles[20:]}-"
                f":{intervals}"
            )
            self._gtids.append(gtid)

        self._previous_gtids = ",".join(self._gtids)

    def _dump(self):
        print(f"previous_gtids: {self._previous_gtids}")

    def __repr__(self):
        return f'<PreviousGtidsEvent "{self._previous_gtids}">'


class MariadbGtidEvent(BinLogEvent):
    """
    GTID(Global Transaction Identifier) change in binlog event in MariaDB

    For more information: `[see details] <https://mariadb.com/kb/en/gtid_event/>`_.

    :ivar server_id: int - The ID of the server where the GTID event occurred.
    :ivar gtid_seq_no: int - The sequence number of the GTID event.
    :ivar domain_id: int - The domain ID associated with the GTID event.
    :ivar flags: int - Flags related to the GTID event.
    :ivar gtid: str - The Global Transaction Identifier in the format ‘domain_id-server_id-gtid_seq_no’.
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        self.server_id = self.packet.server_id
        self.gtid_seq_no = self.packet.read_uint64()
        self.domain_id = self.packet.read_uint32()
        self.flags = self.packet.read_uint8()
        self.gtid = f"{self.domain_id}-{self.server_id}-{self.gtid_seq_no}"

    def _dump(self):
        super()._dump()
        print(f"Flags: {self.flags}")
        print(f"GTID: {self.gtid}")


class MariadbBinLogCheckPointEvent(BinLogEvent):
    """
    Represents a checkpoint in a binlog event in MariaDB.

    More details are available in the MariaDB Knowledge Base:
    https://mariadb.com/kb/en/binlog_checkpoint_event/

    :ivar filename_length: int - The length of the filename.
    :ivar filename: str - The name of the file saved at the checkpoint.
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(MariadbBinLogCheckPointEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )
        filename_length = self.packet.read_uint32()
        self.filename = self.packet.read(filename_length).decode()

    def _dump(self):
        print(f"Filename: {self.filename}")


class MariadbAnnotateRowsEvent(BinLogEvent):
    """
    Annotate rows event
    If you want to check this binlog, change the value of the flag(line 382 of the 'binlogstream.py') option to 2
    https://mariadb.com/kb/en/annotate_rows_event/

    :ivar sql_statement: str - The SQL statement
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        self.sql_statement = self.packet.read(event_size)

    def _dump(self):
        super()._dump()
        print(f"SQL statement : {self.sql_statement}")


class MariadbGtidListEvent(BinLogEvent):
    """
    GTID List event
    https://mariadb.com/kb/en/gtid_list_event/

    :ivar gtid_length: int - Number of GTIDs
    :ivar gtid_list: list - list of 'MariadbGtidObejct'

    'MariadbGtidObejct' Attributes:
        domain_id: Replication Domain ID
        server_id: Server_ID
        gtid_seq_no: GTID sequence
        gtid: 'domain_id'+ 'server_id' + 'gtid_seq_no'
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(MariadbGtidListEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        class MariadbGtidObejct(BinLogEvent):
            """
            Information class of elements in GTID list
            """

            def __init__(
                self, from_packet, event_size, table_map, ctl_connection, **kwargs
            ):
                super(MariadbGtidObejct, self).__init__(
                    from_packet, event_size, table_map, ctl_connection, **kwargs
                )
                self.domain_id = self.packet.read_uint32()
                self.server_id = self.packet.read_uint32()
                self.gtid_seq_no = self.packet.read_uint64()
                self.gtid = f"{self.domain_id}-{self.server_id}-{self.gtid_seq_no}"

        self.gtid_length = self.packet.read_uint32()
        self.gtid_list = [
            MariadbGtidObejct(
                from_packet, event_size, table_map, ctl_connection, **kwargs
            )
            for i in range(self.gtid_length)
        ]


class RotateEvent(BinLogEvent):
    """
    Change MySQL bin log file
    Represents information for the slave to know the name of the binary log it is going to receive.

    For more information: `[see details] <https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Rotate__event.html>`_.

    :ivar position: int - Position inside next binlog
    :ivar next_binlog: str - Name of next binlog file
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        self.position = struct.unpack("<Q", self.packet.read(8))[0]
        self.next_binlog = self.packet.read(event_size - 8).decode()

    def dump(self):
        print(f"=== {self.__class__.__name__} ===")
        print(f"Position: {self.position}")
        print(f"Next binlog file: {self.next_binlog}")
        print()


class XAPrepareEvent(BinLogEvent):
    """
    An XA prepare event is generated for a XA prepared transaction.
    Like Xid_event, it contains XID of the **prepared** transaction.

    For more information: `[see details] <https://dev.mysql.com/doc/refman/8.0/en/xa-statements.html>`_.

    :ivar one_phase: current XA transaction commit method
    :ivar xid_format_id: a number that identifies the format used by the gtrid and bqual values
    :ivar xid: serialized XID representation of XA transaction (xid_gtrid + xid_bqual)
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        # one_phase is True: XA COMMIT ... ONE PHASE
        # one_phase is False: XA PREPARE
        self.one_phase = self.packet.read(1) != b"\x00"
        self.xid_format_id = struct.unpack("<I", self.packet.read(4))[0]
        gtrid_length = struct.unpack("<I", self.packet.read(4))[0]
        bqual_length = struct.unpack("<I", self.packet.read(4))[0]
        self.xid_gtrid = self.packet.read(gtrid_length)
        self.xid_bqual = self.packet.read(bqual_length)

    @property
    def xid(self):
        return self.xid_gtrid.decode() + self.xid_bqual.decode()

    def _dump(self):
        print(f"One phase: {self.one_phase}")
        print(f"XID formatID: {self.xid_format_id}")
        print(f"XID: {self.xid}")


class FormatDescriptionEvent(BinLogEvent):
    """
    Represents a Format Description Event in the MySQL binary log.

    This event is written at the start of a binary log file for binlog version 4.
    It provides the necessary information to decode subsequent events in the file.

    :ivar binlog_version: int - Version of the binary log format.
    :ivar mysql_version_str: str - Server's MySQL version in string format.
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        self.binlog_version = struct.unpack("<H", self.packet.read(2))
        self.mysql_version_str = self.packet.read(50).rstrip(b"\0").decode()
        numbers = self.mysql_version_str.split("-")[0]
        self.mysql_version = tuple(map(int, numbers.split(".")))
        self.created = struct.unpack("<I", self.packet.read(4))[0]
        self.common_header_len = struct.unpack("<B", self.packet.read(1))[0]
        offset = (
            4 + 2 + 50 + 1
        )  # created + binlog_version + mysql_version_str + common_header_len
        checksum_algorithm = 1
        checksum = 4
        n = event_size - offset - self.common_header_len - checksum_algorithm - checksum
        self.post_header_len = struct.unpack(f"<{n}B", self.packet.read(n))
        self.server_version_split = struct.unpack("<3B", self.packet.read(3))
        self.number_of_event_types = struct.unpack("<B", self.packet.read(1))[0]

    def _dump(self):
        print(f"Binlog version: {self.binlog_version}")
        print(f"mysql version: {self.mysql_version_str}")
        print(f"Created: {self.created}")
        print(f"Common header length: {self.common_header_len}")
        print(f"Post header length: {self.post_header_len}")
        print(f"Server version split: {self.server_version_split}")
        print(f"Number of event types: {self.number_of_event_types}")


class StopEvent(BinLogEvent):
    pass


class XidEvent(BinLogEvent):
    """
    A COMMIT event generated when COMMIT of a transaction that modifies one or more tables of an XA-capable storage engine occurs.

    For more information: `[see details] <https://mariadb.com/kb/en/xid_event/>`_.

    :ivar xid: uint - Transaction ID for 2 Phase Commit.
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        self.xid = struct.unpack("<Q", self.packet.read(8))[0]

    def _dump(self):
        super()._dump()
        print(f"Transaction ID: {self.xid}")


class HeartbeatLogEvent(BinLogEvent):
    """A Heartbeat event
    Heartbeats are sent by the master.
    Master sends heartbeats when there are no unsent events in the binary log file after certain period of time.
    The interval is defined by MASTER_HEARTBEAT_PERIOD connection setting.

    `[see MASTER_HEARTBEAT_PERIOD] <https://dev.mysql.com/doc/refman/8.0/en/change-master-to.html>`_.

    A Mysql server also does it for each skipped events in the log.
    This is because to make the slave bump its position so that
    if a disconnection occurs, the slave will only reconnects from the lasted skipped position. (Baloo's idea)

    (see Binlog_sender::send_events in sql/rpl_binlog_sender.cc)

    Warning:
    That makes 106 bytes of data for skipped event in the binlog.
    *this is also the case with GTID replication*.
    To mitigate such behavior, you are expected to keep the binlog small
    (see max_binlog_size, defaults to 1G).
    In any case, the timestamp is 0 (as in 1970-01-01T00:00:00).

    :ivar ident: Name of the current binlog
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        self.ident = self.packet.read(event_size).decode()

    def _dump(self):
        super()._dump()
        print(f"Current binlog: {self.ident}")


class QueryEvent(BinLogEvent):
    """
    QueryEvent is generated for each query that modified database.
    If row-based replication is used, DML will not be logged as RowsEvent instead.

    :ivar slave_proxy_id: int - The id of the thread that issued this statement on the master server
    :ivar execution_time: int - The time from when the query started to when it was logged in the binlog, in seconds.
    :ivar schema_length: int - The length of the name of the currently selected database.
    :ivar error_code: int - Error code generated by the master
    :ivar status_vars_length: int - The length of the status variable

    :ivar schema: str - The name of the currently selected database.
    :ivar query: str - The query executed.
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        # Post-header
        self.slave_proxy_id = self.packet.read_uint32()
        self.execution_time = self.packet.read_uint32()
        self.schema_length = struct.unpack("!B", self.packet.read(1))[0]
        self.error_code = self.packet.read_uint16()
        self.status_vars_length = self.packet.read_uint16()

        # Payload
        status_vars_end_pos = self.packet.read_bytes + self.status_vars_length
        while self.packet.read_bytes < status_vars_end_pos:
            # read KEY for status variable
            status_vars_key = self.packet.read_uint8()
            # read VALUE for status variable
            self._read_status_vars_value_for_key(status_vars_key)

        self.schema = self.packet.read(self.schema_length)
        self.packet.advance(1)
        query = self.packet.read(
            event_size - 13 - self.status_vars_length - self.schema_length - 1
        )
        self.query = query.decode("utf-8", errors="backslashreplace")
        # string[EOF]    query

    def _dump(self):
        super()._dump()
        print(f"Schema: {self.schema}" % (self.schema))
        print(f"Execution time: {self.execution_time}")
        print(f"Query: {self.query}")

    def _read_status_vars_value_for_key(self, key):
        """parse status variable VALUE for given KEY

        A status variable in query events is a sequence of status KEY-VALUE pairs.
        Parsing logic from mysql-server source code edited by dongwook-chan
        https://github.com/mysql/mysql-server/blob/beb865a960b9a8a16cf999c323e46c5b0c67f21f/libbinlogevents/src/statement_events.cpp#L181-L336

        :ivar key: key for status variable
        """
        if key == Q_FLAGS2_CODE:  # 0x00
            self.flags2 = self.packet.read_uint32()
        elif key == Q_SQL_MODE_CODE:  # 0x01
            self.sql_mode = self.packet.read_uint64()
        elif key == Q_CATALOG_CODE:  # 0x02 for MySQL 5.0.x
            pass
        elif key == Q_AUTO_INCREMENT:  # 0x03
            self.auto_increment_increment = self.packet.read_uint16()
            self.auto_increment_offset = self.packet.read_uint16()
        elif key == Q_CHARSET_CODE:  # 0x04
            self.character_set_client = self.packet.read_uint16()
            self.collation_connection = self.packet.read_uint16()
            self.collation_server = self.packet.read_uint16()
        elif key == Q_TIME_ZONE_CODE:  # 0x05
            time_zone_len = self.packet.read_uint8()
            if time_zone_len:
                self.time_zone = self.packet.read(time_zone_len)
        elif key == Q_CATALOG_NZ_CODE:  # 0x06
            catalog_len = self.packet.read_uint8()
            if catalog_len:
                self.catalog_nz_code = self.packet.read(catalog_len)
        elif key == Q_LC_TIME_NAMES_CODE:  # 0x07
            self.lc_time_names_number = self.packet.read_uint16()
        elif key == Q_CHARSET_DATABASE_CODE:  # 0x08
            self.charset_database_number = self.packet.read_uint16()
        elif key == Q_TABLE_MAP_FOR_UPDATE_CODE:  # 0x09
            self.table_map_for_update = self.packet.read_uint64()
        elif key == Q_MASTER_DATA_WRITTEN_CODE:  # 0x0A
            pass
        elif key == Q_INVOKER:  # 0x0B
            user_len = self.packet.read_uint8()
            if user_len:
                self.user = self.packet.read(user_len)
            host_len = self.packet.read_uint8()
            if host_len:
                self.host = self.packet.read(host_len)
        elif key == Q_UPDATED_DB_NAMES:  # 0x0C
            mts_accessed_dbs = self.packet.read_uint8()
            """
            mts_accessed_dbs < 254:
                `mts_accessed_dbs` is equal to the number of dbs
                accessed by the query event.
            mts_accessed_dbs == 254:
                This is the case where the number of dbs accessed
                is 1 and the name of the only db is ""
                Since no further parsing required(empty name), return.
            """
            if mts_accessed_dbs == 254:
                return
            dbs = []
            for i in range(mts_accessed_dbs):
                db = self.packet.read_string()
                dbs.append(db)
            self.mts_accessed_db_names = dbs
        elif key == Q_MICROSECONDS:  # 0x0D
            self.microseconds = self.packet.read_uint24()
        elif key == Q_COMMIT_TS:  # 0x0E
            pass
        elif key == Q_COMMIT_TS2:  # 0x0F
            pass
        elif key == Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:  # 0x10
            self.explicit_defaults_ts = self.packet.read_uint8()
        elif key == Q_DDL_LOGGED_WITH_XID:  # 0x11
            self.ddl_xid = self.packet.read_uint64()
        elif key == Q_DEFAULT_COLLATION_FOR_UTF8MB4:  # 0x12
            self.default_collation_for_utf8mb4_number = self.packet.read_uint16()
        elif key == Q_SQL_REQUIRE_PRIMARY_KEY:  # 0x13
            self.sql_require_primary_key = self.packet.read_uint8()
        elif key == Q_DEFAULT_TABLE_ENCRYPTION:  # 0x14
            self.default_table_encryption = self.packet.read_uint8()
        elif key == Q_HRNOW:
            self.hrnow = self.packet.read_uint24()
        elif key == Q_XID:
            self.xid = self.packet.read_uint64()
        else:
            raise StatusVariableMismatch


class BeginLoadQueryEvent(BinLogEvent):
    """
    This event is written into the binary log file for LOAD DATA INFILE events
    if the server variable binlog_mode was set to "STATEMENT".

    :ivar file_id: the id of the file
    :ivar block-data: data block about "LOAD DATA INFILE"
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        # Payload
        self.file_id = self.packet.read_uint32()
        self.block_data = self.packet.read(event_size - 4)

    def _dump(self):
        super()._dump()
        print(f"File id: {self.file_id}")
        print(f"Block data: {self.block_data}")


class ExecuteLoadQueryEvent(BinLogEvent):
    """
    This event handles "LOAD DATA INFILE" statement.
    LOAD DATA INFILE statement reads data from file and insert into database's table.
    Since QueryEvent cannot explain this special action, ExecuteLoadQueryEvent is needed.
    So it is similar to a QUERY_EVENT except that it has extra static fields.

    :ivar slave_proxy_id: int - The id of the thread that issued this statement on the master server
    :ivar execution_time: int - The number of seconds that the statement took to execute
    :ivar schema_length: int - The length of the default database's name when the statement was executed.
    :ivar error_code: int - The error code resulting from execution of the statement on the master
    :ivar status_vars_length: int - The length of the status variable block
    :ivar file_id: int - The id of the loaded file
    :ivar start_pos: int - Offset from the start of the statement to the beginning of the filename
    :ivar end_pos: int - Offset from the start of the statement to the end of the filename
    :ivar dup_handling_flags: int - How LOAD DATA INFILE handles duplicated data (0x0: error, 0x1: ignore, 0x2: replace)
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        # Post-header
        self.slave_proxy_id = self.packet.read_uint32()
        self.execution_time = self.packet.read_uint32()
        self.schema_length = self.packet.read_uint8()
        self.error_code = self.packet.read_uint16()
        self.status_vars_length = self.packet.read_uint16()

        # Payload
        self.file_id = self.packet.read_uint32()
        self.start_pos = self.packet.read_uint32()
        self.end_pos = self.packet.read_uint32()
        self.dup_handling_flags = self.packet.read_uint8()

    def _dump(self):
        super(ExecuteLoadQueryEvent, self)._dump()
        print(f"Slave proxy id: {self.slave_proxy_id}")
        print(f"Execution time: {self.execution_time}")
        print(f"Schema length: {self.schema_length}")
        print(f"Error code: {self.error_code}")
        print(f"Status vars length: {self.status_vars_length}")
        print(f"File id: {self.file_id}")
        print(f"Start pos: {self.start_pos}")
        print(f"End pos: {self.end_pos}")
        print(f"Dup handling flags: {self.dup_handling_flags}")


class IntvarEvent(BinLogEvent):
    """
    Stores the value of auto-increment variables.
    This event will be created just before a QueryEvent.

    :ivar type: int - 1 byte identifying the type of variable stored.
    Can be either LAST_INSERT_ID_EVENT (1) or INSERT_ID_EVENT (2).
    :ivar value: int - The value of the variable
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        # Payload
        self.type = self.packet.read_uint8()
        self.value = self.packet.read_uint32()

    def _dump(self):
        super()._dump()
        print(f"type: {self.type}")
        print(f"Value: {self.value}")


class RandEvent(BinLogEvent):
    """
    RandEvent is generated every time a statement uses the RAND() function.
    Indicates the seed values to use for generating a random number with RAND() in the next statement.

    RandEvent only works in statement-based logging (need to set binlog_format as 'STATEMENT')
    and only works when the seed number is not specified.

    :ivar seed1: int - value for the first seed
    :ivar seed2: int - value for the second seed
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        # Payload
        self._seed1 = self.packet.read_uint64()
        self._seed2 = self.packet.read_uint64()

    @property
    def seed1(self):
        """Get the first seed value"""
        return self._seed1

    @property
    def seed2(self):
        """Get the second seed value"""
        return self._seed2

    def _dump(self):
        super()._dump()
        print(f"seed1: {self.seed1}")
        print(f"seed2: {self.seed2}")


class UserVarEvent(BinLogEvent):
    """
    UserVarEvent is generated every time a statement uses a user variable.
    Indicates the value to use for the user variable in the next statement.

    :ivar name_len: int - Length of user variable
    :ivar name: str - User variable name
    :ivar value: str - Value of the user variable
    :ivar type: int - Type of the user variable
    :ivar charset: int - The number of the character set for the user variable
    :ivar is_null: int - Non-zero if the variable value is the SQL NULL value, 0 otherwise
    :ivar flags: int - Extra flags associated with the user variable
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(UserVarEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        # Payload
        self.name_len: int = self.packet.read_uint32()
        self.name: str = self.packet.read(self.name_len).decode()
        self.is_null: int = self.packet.read_uint8()
        self.type_to_codes_and_method: dict = {
            0x00: ["STRING_RESULT", self._read_string],
            0x01: ["REAL_RESULT", self._read_real],
            0x02: ["INT_RESULT", self._read_int],
            0x03: ["ROW_RESULT", self._read_default],
            0x04: ["DECIMAL_RESULT", self._read_decimal],
        }

        self.value: Optional[Union[str, float, int, decimal.Decimal]] = None
        self.flags: Optional[int] = None
        self.temp_value_buffer: Union[bytes, memoryview] = b""

        if not self.is_null:
            self.type: int = self.packet.read_uint8()
            self.charset: int = self.packet.read_uint32()
            self.value_len: int = self.packet.read_uint32()
            self.temp_value_buffer: Union[bytes, memoryview] = self.packet.read(
                self.value_len
            )
            self.flags: int = self.packet.read_uint8()
            self._set_value_from_temp_buffer()
        else:
            self.type, self.charset, self.value_len, self.value, self.flags = (
                None,
                None,
                None,
                None,
                None,
            )

    def _set_value_from_temp_buffer(self):
        """
        Set the value from the temporary buffer based on the type code.
        """
        if self.temp_value_buffer:
            type_code, read_method = self.type_to_codes_and_method.get(
                self.type, ["UNKNOWN_RESULT", self._read_default]
            )
            if type_code == "INT_RESULT":
                self.value = read_method(self.temp_value_buffer, self.flags)
            else:
                self.value = read_method(self.temp_value_buffer)

    def _read_string(self, buffer: bytes) -> str:
        """
        Read string data.
        """
        return buffer.decode()

    def _read_real(self, buffer: bytes) -> float:
        """
        Read real data.
        """
        return struct.unpack("<d", buffer)[0]

    def _read_int(self, buffer: bytes, flags: int) -> int:
        """
        Read integer data.
        """
        fmt = "<Q" if flags == 1 else "<q"
        return struct.unpack(fmt, buffer)[0]

    def _read_decimal(self, buffer: bytes) -> decimal.Decimal:
        """
        Read decimal data.
        """
        self.precision = self.temp_value_buffer[0]
        self.decimals = self.temp_value_buffer[1]
        raw_decimal = self.temp_value_buffer[2:]
        return parse_decimal_from_bytes(raw_decimal, self.precision, self.decimals)

    def _read_default(self) -> bytes:
        """
        Read default data.
        Used when the type is None.
        """
        return self.packet.read(self.value_len)

    def _dump(self) -> None:
        super(UserVarEvent, self)._dump()
        print(f"User variable name: {self.name}")
        print(f'Is NULL: {"Yes" if self.is_null else "No"}')
        if not self.is_null:
            print(
                f'Type: {self.type_to_codes_and_method.get(self.type, ["UNKNOWN_TYPE"])[0]}'
            )
            print(f"Charset: {self.charset}")
            print(f"Value: {self.value}")
            print(f"Flags: {self.flags}")


class MariadbStartEncryptionEvent(BinLogEvent):
    """
    Since MariaDB 10.1.7,
    the START_ENCRYPTION event is written to every binary log file
    if encrypt_binlog is set to ON. Prior to enabling this setting,
    additional configuration steps are required in MariaDB.
    (Link: https://mariadb.com/kb/en/encrypting-binary-logs/)

    This event is written just once, after the Format Description event

    Attributes:
        schema: The Encryption scheme, always set to 1 for system files.
        key_version: The Encryption key version.
        nonce: Nonce (12 random bytes) of current binlog file.
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)

        self.schema = self.packet.read_uint8()
        self.key_version = self.packet.read_uint32()
        self.nonce = self.packet.read(12)

    def _dump(self):
        print(f"Schema: {self.schema}")
        print(f"Key version: {self.key_version}")
        print(f"Nonce: {self.nonce}")


class RowsQueryLogEvent(BinLogEvent):
    """
    Record original query for the row events in Row-Based Replication

    More details are available in the MySQL Knowledge Base:
    https://dev.mysql.com/doc/dev/mysql-server/latest/classRows__query__log__event.html

    :ivar query: str - The executed SQL statement
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(RowsQueryLogEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )
        self.packet.advance(1)
        self.query = self.packet.read_available().decode("utf-8")

    def dump(self):
        print(f"=== {self.__class__.__name__} ===")
        print(f"Query: {self.query}")


class NotImplementedEvent(BinLogEvent):
    """
    Used as a temporary class for events that have not yet been implemented.

    The event referencing this class skips parsing.
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        self.packet.advance(event_size)
