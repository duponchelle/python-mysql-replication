import struct
import logging
from packaging.version import Version

import pymysql
from pymysql.constants.COMMAND import COM_BINLOG_DUMP, COM_REGISTER_SLAVE
from pymysql.cursors import DictCursor

from .constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT, FORMAT_DESCRIPTION_EVENT
from .event import (
    QueryEvent,
    RotateEvent,
    FormatDescriptionEvent,
    XidEvent,
    GtidEvent,
    StopEvent,
    XAPrepareEvent,
    BeginLoadQueryEvent,
    ExecuteLoadQueryEvent,
    HeartbeatLogEvent,
    NotImplementedEvent,
    MariadbGtidEvent,
    MariadbAnnotateRowsEvent,
    RandEvent,
    MariadbStartEncryptionEvent,
    RowsQueryLogEvent,
    MariadbGtidListEvent,
    MariadbBinLogCheckPointEvent,
    UserVarEvent,
    PreviousGtidsEvent,
)
from .exceptions import BinLogNotEnabled
from .gtid import GtidSet
from .packet import BinLogPacketWrapper
from .row_event import (
    UpdateRowsEvent,
    WriteRowsEvent,
    DeleteRowsEvent,
    TableMapEvent,
    PartialUpdateRowsEvent,
)

try:
    from pymysql.constants.COMMAND import COM_BINLOG_DUMP_GTID
except ImportError:
    # Handle old pymysql versions
    # See: https://github.com/PyMySQL/PyMySQL/pull/261
    COM_BINLOG_DUMP_GTID = 0x1E

# 2013 Connection Lost
# 2006 MySQL server has gone away
MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]

PYMYSQL_VERSION_LT_06 = Version(pymysql.__version__) < Version("0.6")


class ReportSlave(object):
    """Represent the values that you may report when connecting as a slave
    to a master. SHOW SLAVE HOSTS related"""

    hostname = ""
    username = ""
    password = ""
    port = 0

    def __init__(self, value):
        """
        Attributes:
            value: string or tuple
                   if string, then it will be used hostname
                   if tuple it will be used as (hostname, user, password, port)
        """

        if isinstance(value, (tuple, list)):
            try:
                self.hostname = value[0]
                self.username = value[1]
                self.password = value[2]
                self.port = int(value[3])
            except IndexError:
                pass
        elif isinstance(value, dict):
            for key in ["hostname", "username", "password", "port"]:
                try:
                    setattr(self, key, value[key])
                except KeyError:
                    pass
        else:
            self.hostname = value

    def __repr__(self):
        return (
            f"<ReportSlave hostname={self.hostname} "
            f"username={self.username} "
            f"password={self.password} "
            f"port={self.port}>"
        )

    def encoded(self, server_id, master_id=0):
        """
        server_id: the slave server-id
        master_id: usually 0. Appears as "master id" in SHOW SLAVE HOSTS
                   on the master. Unknown what else it impacts.
        """

        # 1              [15] COM_REGISTER_SLAVE
        # 4              server-id
        # 1              slaves hostname length
        # string[$len]   slaves hostname
        # 1              slaves user len
        # string[$len]   slaves user
        # 1              slaves password len
        # string[$len]   slaves password
        # 2              slaves mysql-port
        # 4              replication rank
        # 4              master-id

        lhostname = len(self.hostname.encode())
        lusername = len(self.username.encode())
        lpassword = len(self.password.encode())

        packet_len = (
            1
            + 4  # command
            + 1  # server-id
            + lhostname  # hostname length
            + 1
            + lusername  # username length
            + 1
            + lpassword  # password length
            + 2
            + 4  # slave mysql port
            + 4  # replication rank
        )  # master-id

        MAX_STRING_LEN = 257  # one byte for length + 256 chars

        return (
            struct.pack("<i", packet_len)
            + bytes(bytearray([COM_REGISTER_SLAVE]))
            + struct.pack("<L", server_id)
            + struct.pack(
                f"<{min(MAX_STRING_LEN, lhostname + 1)}p", self.hostname.encode()
            )
            + struct.pack(
                f"<{min(MAX_STRING_LEN, lusername + 1)}p", self.username.encode()
            )
            + struct.pack(
                f"<{min(MAX_STRING_LEN, lpassword + 1)}p", self.password.encode()
            )
            + struct.pack("<H", self.port)
            + struct.pack("<l", 0)
            + struct.pack("<l", master_id)
        )


class BinLogStreamReader(object):
    """Connect to replication stream and read event"""

    report_slave = None

    def __init__(
        self,
        connection_settings,
        server_id,
        ctl_connection_settings=None,
        resume_stream=False,
        blocking=False,
        only_events=None,
        log_file=None,
        log_pos=None,
        end_log_pos=None,
        filter_non_implemented_events=True,
        ignored_events=None,
        auto_position=None,
        only_tables=None,
        ignored_tables=None,
        only_schemas=None,
        ignored_schemas=None,
        freeze_schema=False,
        skip_to_timestamp=None,
        report_slave=None,
        slave_uuid=None,
        pymysql_wrapper=None,
        slave_heartbeat=None,
        is_mariadb=False,
        annotate_rows_event=False,
        ignore_decode_errors=False,
        verify_checksum=False,
        enable_logging=True,
    ):
        """
        Attributes:
            ctl_connection_settings: Connection settings for cluster holding
                                     schema information
            resume_stream: Start for event from position or the latest event of
                           binlog or from older available event
            blocking: When master has finished reading/sending binlog it will
                      send EOF instead of blocking connection.
            only_events: Array of allowed events
            ignored_events: Array of ignored events
            log_file: Set replication start log file
            log_pos: Set replication start log pos (resume_stream should be
                     true)
            end_log_pos: Set replication end log pos
            auto_position: Use master_auto_position gtid to set position
            only_tables: An array with the tables you want to watch (only works
                         in binlog_format ROW)
            ignored_tables: An array with the tables you want to skip
            only_schemas: An array with the schemas you want to watch
            ignored_schemas: An array with the schemas you want to skip
            freeze_schema: If true do not support ALTER TABLE. It's faster.
            skip_to_timestamp: Ignore all events until reaching specified
                               timestamp.
            report_slave: Report slave in SHOW SLAVE HOSTS.
            slave_uuid: Report slave_uuid or replica_uuid in SHOW SLAVE HOSTS(MySQL 8.0.21-) or
                        SHOW REPLICAS(MySQL 8.0.22+) depends on your MySQL version.
            slave_heartbeat: (seconds) Should master actively send heartbeat on
                             connection. This also reduces traffic in GTID
                             replication on replication resumption (in case
                             many event to skip in binlog). See
                             MASTER_HEARTBEAT_PERIOD in mysql documentation
                             for semantics
            is_mariadb: Flag to indicate it's a MariaDB server, used with auto_position
                    to point to Mariadb specific GTID.
            annotate_rows_event: Parameter value to enable annotate rows event in mariadb,
                    used with 'is_mariadb'
            ignore_decode_errors: If true, any decode errors encountered
                                  when reading column data will be ignored.
            verify_checksum: If true, verify events read from the binary log by examining checksums.
            enable_logging: When set to True, logs various details helpful for debugging and monitoring
                            When set to False, logging is disabled to enhance performance.
        """

        self.__connection_settings = connection_settings
        self.__connection_settings.setdefault("charset", "utf8")

        self.__connected_stream = False
        self.__connected_ctl = False
        self.__resume_stream = resume_stream
        self.__blocking = blocking
        self._ctl_connection_settings = ctl_connection_settings
        if ctl_connection_settings:
            self._ctl_connection_settings.setdefault("charset", "utf8")

        self.__only_tables = only_tables
        self.__ignored_tables = ignored_tables
        self.__only_schemas = only_schemas
        self.__ignored_schemas = ignored_schemas
        self.__freeze_schema = freeze_schema
        self.__allowed_events = self._allowed_event_list(
            only_events, ignored_events, filter_non_implemented_events
        )
        self.__ignore_decode_errors = ignore_decode_errors
        self.__verify_checksum = verify_checksum
        self.__optional_meta_data = False

        # We can't filter on packet level TABLE_MAP and rotate event because
        # we need them for handling other operations
        self.__allowed_events_in_packet = frozenset([TableMapEvent, RotateEvent]).union(
            self.__allowed_events
        )

        self.__server_id = server_id
        self.__use_checksum = False

        # Store table meta information
        self.table_map = {}
        self.log_pos = log_pos
        self.end_log_pos = end_log_pos
        self.log_file = log_file
        self.auto_position = auto_position
        self.skip_to_timestamp = skip_to_timestamp
        self.is_mariadb = is_mariadb
        self.__annotate_rows_event = annotate_rows_event
        if enable_logging:
            self.__log_valid_parameters()

        if end_log_pos:
            self.is_past_end_log_pos = False

        if report_slave:
            self.report_slave = ReportSlave(report_slave)
        self.slave_uuid = slave_uuid
        self.slave_heartbeat = slave_heartbeat

        if pymysql_wrapper:
            self.pymysql_wrapper = pymysql_wrapper
        else:
            self.pymysql_wrapper = pymysql.connect
        self.mysql_version = (0, 0, 0)
        self.dbms = None

    def close(self):
        if self.__connected_stream:
            self._stream_connection.close()
            self.__connected_stream = False
        if self.__connected_ctl:
            # break reference cycle between stream reader and underlying
            # mysql connection object
            self._ctl_connection.close()
            self.__connected_ctl = False

    def __connect_to_ctl(self):
        if not self._ctl_connection_settings:
            self._ctl_connection_settings = dict(self.__connection_settings)
        self._ctl_connection_settings["db"] = "information_schema"
        self._ctl_connection_settings["cursorclass"] = DictCursor
        self._ctl_connection_settings["autocommit"] = True
        self._ctl_connection = self.pymysql_wrapper(**self._ctl_connection_settings)
        self._ctl_connection._get_dbms = self.__get_dbms
        self.__connected_ctl = True
        self.__check_optional_meta_data()

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        cur = self._stream_connection.cursor()
        cur.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
        result = cur.fetchone()
        cur.close()

        if result is None:
            return False
        var, value = result[:2]
        if value == "NONE":
            return False
        return True

    def _register_slave(self):
        if not self.report_slave:
            return

        packet = self.report_slave.encoded(self.__server_id)

        if PYMYSQL_VERSION_LT_06:
            self._stream_connection.wfile.write(packet)
            self._stream_connection.wfile.flush()
            self._stream_connection.read_packet()
        else:
            self._stream_connection._write_bytes(packet)
            self._stream_connection._next_seq_id = 1
            self._stream_connection._read_packet()

    def __connect_to_stream(self):
        # log_pos (4) -- position in the binlog-file to start the stream with
        # flags (2) BINLOG_DUMP_NON_BLOCK (0 or 1)
        # server_id (4) -- server id of this slave
        # log_file (string.EOF) -- filename of the binlog on the master
        self._stream_connection = self.pymysql_wrapper(**self.__connection_settings)

        self.__use_checksum = self.__checksum_enabled()

        # If checksum is enabled we need to inform the server about the that
        # we support it
        if self.__use_checksum:
            cur = self._stream_connection.cursor()
            cur.execute("SET @master_binlog_checksum= @@global.binlog_checksum")
            cur.close()

        if self.slave_uuid:
            cur = self._stream_connection.cursor()
            cur.execute(
                f"SET @slave_uuid = '{self.slave_uuid}', @replica_uuid = '{self.slave_uuid}'"
            )
            cur.close()

        if self.slave_heartbeat:
            # 4294967 is documented as the max value for heartbeats
            net_timeout = float(self.__connection_settings.get("read_timeout", 4294967))
            # If heartbeat is too low, the connection will disconnect before,
            # this is also the behavior in mysql
            heartbeat = float(min(net_timeout / 2.0, self.slave_heartbeat))
            if heartbeat > 4294967:
                heartbeat = 4294967

            # master_heartbeat_period is nanoseconds
            heartbeat = int(heartbeat * 1000000000)
            cur = self._stream_connection.cursor()
            cur.execute_with_args("SET @master_heartbeat_period= %d", (heartbeat,))
            cur.close()

        # When replicating from Mariadb 10.6.12 using binlog coordinates, a slave capability < 4 triggers a bug in
        # Mariadb, when it tries to replace GTID events with dummy ones. Given that this library understands GTID
        # events, setting the capability to 4 circumvents this error.
        # If the DB is mysql, this won't have any effect so no need to run this in a condition
        cur = self._stream_connection.cursor()
        cur.execute("SET @mariadb_slave_capability=4")
        cur.close()

        self._register_slave()

        if not self.auto_position:
            if self.is_mariadb:
                prelude = self.__set_mariadb_settings()
            else:
                # only when log_file and log_pos both provided, the position info is
                # valid, if not, get the current position from master
                if self.log_file is None or self.log_pos is None:
                    cur = self._stream_connection.cursor()
                    cur.execute("SHOW MASTER STATUS")
                    master_status = cur.fetchone()
                    if master_status is None:
                        raise BinLogNotEnabled()
                    self.log_file, self.log_pos = master_status[:2]
                    cur.close()

                prelude = struct.pack("<i", len(self.log_file) + 11) + bytes(
                    bytearray([COM_BINLOG_DUMP])
                )

                if self.__resume_stream:
                    prelude += struct.pack("<I", self.log_pos)
                else:
                    prelude += struct.pack("<I", 4)

                flags = 0

                if not self.__blocking:
                    flags |= 0x01  # BINLOG_DUMP_NON_BLOCK

                prelude += struct.pack("<H", flags)

                prelude += struct.pack("<I", self.__server_id)

                prelude += self.log_file.encode()
        else:
            if self.is_mariadb:
                prelude = self.__set_mariadb_settings()
            else:
                # Format for mysql packet master_auto_position
                #
                # All fields are little endian
                # All fields are unsigned

                # Packet length   uint   4bytes
                # Packet type     byte   1byte   == 0x1e
                # Binlog flags    ushort 2bytes  == 0 (for retrocompatibilty)
                # Server id       uint   4bytes
                # binlognamesize  uint   4bytes
                # binlogname      str    Nbytes  N = binlognamesize
                #                                Zeroified
                # binlog position uint   4bytes  == 4
                # payload_size    uint   4bytes

                # What come next, is the payload, where the slave gtid_executed
                # is sent to the master
                # n_sid           ulong  8bytes  == which size is the gtid_set
                # | sid           uuid   16bytes UUID as a binary
                # | n_intervals   ulong  8bytes  == how many intervals are sent
                # |                                 for this gtid
                # | | start       ulong  8bytes  Start position of this interval
                # | | stop        ulong  8bytes  Stop position of this interval

                # A gtid set looks like:
                #   19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10,
                #   1c2aad49-ae92-409a-b4df-d05a03e4702e:42-47:80-100:130-140
                #
                # In this particular gtid set,
                # 19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10
                # is the first member of the set, it is called a gtid.
                # In this gtid, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457 is the sid
                # and have two intervals, 1-3 and 8-10, 1 is the start position of
                # the first interval 3 is the stop position of the first interval.

                gtid_set = GtidSet(self.auto_position)
                encoded_data_size = gtid_set.encoded_length

                header_size = (
                    2
                    + 4  # binlog_flags
                    + 4  # server_id
                    + 4  # binlog_name_info_size
                    + 8  # empty binlog name
                    + 4  # binlog_pos_info_size
                )  # encoded_data_size

                prelude = (
                    b""
                    + struct.pack("<i", header_size + encoded_data_size)
                    + bytes(bytearray([COM_BINLOG_DUMP_GTID]))
                )

                flags = 0
                if not self.__blocking:
                    flags |= 0x01  # BINLOG_DUMP_NON_BLOCK
                flags |= 0x04  # BINLOG_THROUGH_GTID

                # binlog_flags (2 bytes)
                # see:
                #  https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
                prelude += struct.pack("<H", flags)

                # server_id (4 bytes)
                prelude += struct.pack("<I", self.__server_id)
                # binlog_name_info_size (4 bytes)
                prelude += struct.pack("<I", 3)
                # empty_binlog_namapprovale (4 bytes)
                prelude += b"\0\0\0"
                # binlog_pos_info (8 bytes)
                prelude += struct.pack("<Q", 4)

                # encoded_data_size (4 bytes)
                prelude += struct.pack("<I", gtid_set.encoded_length)
                # encoded_data
                prelude += gtid_set.encoded()

        if PYMYSQL_VERSION_LT_06:
            self._stream_connection.wfile.write(prelude)
            self._stream_connection.wfile.flush()
        else:
            self._stream_connection._write_bytes(prelude)
            self._stream_connection._next_seq_id = 1
        self.__connected_stream = True

    def __set_mariadb_settings(self):
        # https://mariadb.com/kb/en/5-slave-registration/
        cur = self._stream_connection.cursor()
        if self.auto_position is not None:
            cur.execute(f'SET @slave_connect_state="{self.auto_position}"')
        cur.execute("SET @slave_gtid_strict_mode=1")
        cur.execute("SET @slave_gtid_ignore_duplicates=0")
        cur.close()

        # https://mariadb.com/kb/en/com_binlog_dump/
        header_size = (
            4
            + 2  # binlog pos
            + 4  # binlog flags
            + 4  # slave server_id,  # requested binlog file name , set it to empty
        )

        prelude = struct.pack("<i", header_size) + bytes(bytearray([COM_BINLOG_DUMP]))

        # binlog pos
        prelude += struct.pack("<i", 4)

        flags = 0

        # Enable annotate rows event
        if self.__annotate_rows_event:
            flags |= 0x02  # BINLOG_SEND_ANNOTATE_ROWS_EVENT

        if not self.__blocking:
            flags |= 0x01  # BINLOG_DUMP_NON_BLOCK

        # binlog flags
        prelude += struct.pack("<H", flags)

        # server id (4 bytes)
        prelude += struct.pack("<I", self.__server_id)

        # empty_binlog_name (4 bytes)
        prelude += b"\0\0\0\0"

        return prelude

    def __check_optional_meta_data(self):
        cur = self._ctl_connection.cursor()
        cur.execute("SHOW VARIABLES LIKE 'BINLOG_ROW_METADATA';")
        value = cur.fetchone()
        if value is None:  # BinLog Variable Not exist It means Not Supported Version
            logging.log(
                logging.WARN,
                """
                    Before using MARIADB 10.5.0 and MYSQL 8.0.14 versions,
                    use python-mysql-replication version Before 1.0 version """,
            )
        else:
            value = value.get("Value", "")
            if value.upper() != "FULL":
                logging.log(
                    logging.WARN,
                    """
                       Setting The Variable Value BINLOG_ROW_METADATA = FULL, BINLOG_ROW_IMAGE = FULL.
                       By Applying this, provide properly mapped column information on UPDATE,DELETE,INSERT.
                        """,
                )
            else:
                self.__optional_meta_data = True

    def fetchone(self):
        while True:
            if self.end_log_pos and self.is_past_end_log_pos:
                return None

            if not self.__connected_stream:
                self.__connect_to_stream()

            if not self.__connected_ctl:
                self.__connect_to_ctl()

            try:
                if PYMYSQL_VERSION_LT_06:
                    pkt = self._stream_connection.read_packet()
                else:
                    pkt = self._stream_connection._read_packet()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self._stream_connection.close()
                    self.__connected_stream = False
                    logging.log(
                        logging.WARN,
                        """
                          A pymysql.OperationalError error occurred, Re-request the connection.
                        """,
                    )
                    continue
                raise

            if pkt.is_eof_packet():
                self.close()
                return None

            if not pkt.is_ok_packet():
                continue

            binlog_event = BinLogPacketWrapper(
                pkt,
                self.table_map,
                self._ctl_connection,
                self.mysql_version,
                self.__use_checksum,
                self.__allowed_events_in_packet,
                self.__only_tables,
                self.__ignored_tables,
                self.__only_schemas,
                self.__ignored_schemas,
                self.__freeze_schema,
                self.__ignore_decode_errors,
                self.__verify_checksum,
                self.__optional_meta_data,
            )

            if binlog_event.event_type == ROTATE_EVENT:
                self.log_pos = binlog_event.event.position
                self.log_file = binlog_event.event.next_binlog
                # Table Id in binlog are NOT persistent in MySQL - they are in-memory identifiers
                # that means that when MySQL master restarts, it will reuse same table id for different tables
                # which will cause errors for us since our in-memory map will try to decode row data with
                # wrong table schema.
                # The fix is to rely on the fact that MySQL will also rotate to a new binlog file every time it
                # restarts. That means every rotation we see *could* be a sign of restart and so potentially
                # invalidates all our cached table id to schema mappings. This means we have to load them all
                # again for each logfile which is potentially wasted effort but we can't really do much better
                # without being broken in restart case
                if binlog_event.timestamp != 0:
                    self.table_map = {}

            elif binlog_event.log_pos:
                self.log_pos = binlog_event.log_pos

            if self.end_log_pos and self.log_pos >= self.end_log_pos:
                # We're currently at, or past, the specified end log position.
                self.is_past_end_log_pos = True

            # This check must not occur before clearing the ``table_map`` as a
            # result of a RotateEvent.
            #
            # The first RotateEvent in a binlog file has a timestamp of
            # zero.  If the server has moved to a new log and not written a
            # timestamped RotateEvent at the end of the previous log, the
            # RotateEvent at the beginning of the new log will be ignored
            # if the caller provided a positive ``skip_to_timestamp``
            # value.  This will result in the ``table_map`` becoming
            # corrupt.
            #
            # https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
            # From the MySQL Internals Manual:
            #
            #   ROTATE_EVENT is generated locally and written to the binary
            #   log on the master. It is written to the relay log on the
            #   slave when FLUSH LOGS occurs, and when receiving a
            #   ROTATE_EVENT from the master. In the latter case, there
            #   will be two rotate events in total originating on different
            #   servers.
            #
            #   There are conditions under which the terminating
            #   log-rotation event does not occur. For example, the server
            #   might crash.
            if (
                self.skip_to_timestamp
                and binlog_event.timestamp < self.skip_to_timestamp
            ):
                continue

            if (
                binlog_event.event_type == TABLE_MAP_EVENT
                and binlog_event.event is not None
            ):
                self.table_map[binlog_event.event.table_id] = (
                    binlog_event.event.get_table()
                )

            # event is none if we have filter it on packet level
            # we filter also not allowed events
            if binlog_event.event is None or (
                binlog_event.event.__class__ not in self.__allowed_events
            ):
                continue

            if binlog_event.event_type == FORMAT_DESCRIPTION_EVENT:
                self.mysql_version = binlog_event.event.mysql_version

            return binlog_event.event

    def _allowed_event_list(
        self, only_events, ignored_events, filter_non_implemented_events
    ):
        if only_events is not None:
            events = set(only_events)
        else:
            events = set(
                (
                    QueryEvent,
                    RotateEvent,
                    StopEvent,
                    FormatDescriptionEvent,
                    XAPrepareEvent,
                    XidEvent,
                    GtidEvent,
                    BeginLoadQueryEvent,
                    ExecuteLoadQueryEvent,
                    UpdateRowsEvent,
                    WriteRowsEvent,
                    DeleteRowsEvent,
                    TableMapEvent,
                    HeartbeatLogEvent,
                    NotImplementedEvent,
                    MariadbGtidEvent,
                    RowsQueryLogEvent,
                    MariadbAnnotateRowsEvent,
                    RandEvent,
                    MariadbStartEncryptionEvent,
                    MariadbGtidListEvent,
                    MariadbBinLogCheckPointEvent,
                    UserVarEvent,
                    PreviousGtidsEvent,
                    PartialUpdateRowsEvent,
                )
            )
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)
        if filter_non_implemented_events:
            try:
                events.remove(NotImplementedEvent)
            except KeyError:
                pass
        return frozenset(events)

    def __get_dbms(self):
        if not self.__connected_ctl:
            self.__connect_to_ctl()
        if self.dbms:
            return self.dbms
        if "MariaDB" in self._ctl_connection.get_server_info():
            self.dbms = "mariadb"
            return "mariadb"
        self.dbms = "mysql"
        return "mysql"

    def __log_valid_parameters(self):
        ignored = ["allowed_events", "table_map"]
        for parameter, value in self.__dict__.items():
            if parameter.startswith("_BinLogStreamReader__"):
                parameter = parameter.replace("_BinLogStreamReader__", "")
            if parameter in ignored or not value:
                continue
            if value is frozenset:
                string_list = [
                    str(item).split()[-1][:-2].split(".")[2] for item in value
                ]
                items = ", ".join(string_list)
                comment = f"{parameter}: [{items}]"
            else:
                comment = f"{parameter}: {value}"
            logging.info(comment)

    def __iter__(self):
        return iter(self.fetchone, None)
