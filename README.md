# python-mysql-replication

<a href="https://pypi.python.org/pypi/mysql-replication"><img src="https://github.com/julien-duponchelle/python-mysql-replication/blob/main/logo.svg"></a>

Pure Python Implementation of MySQL replication protocol build on top of PyMYSQL. This allows you to receive event like insert, update, delete with their datas and raw SQL queries.

# Use cases

- MySQL to NoSQL database replication
- MySQL to search engine replication
- Invalidate cache when something change in database
- Audit
- Real time analytics

# Documentation

A work in progress documentation is available here: https://python-mysql-replication.readthedocs.org/en/latest/

Instruction about building documentation is available here:
https://python-mysql-replication.readthedocs.org/en/latest/developement.html

# Installation

```
pip install mysql-replication
```

# Getting support

You can get support and discuss about new features on:
https://github.com/julien-duponchelle/python-mysql-replication/discussions

# Project status

The project is test with:

- MySQL 5.5, 5.6 and 5.7 (v0.1 ~ v0.45)
- MySQL 8.0.14 (v1.0 ~)
- MariaDB 10.6
- Python 3.7, 3.11
- PyPy 3.7, 3.9 (really faster than the standard Python interpreter)

MySQL version 8.0.14 and later Set global variable binlog_row_metadata='FULL' and binlog_row_image='FULL'

The project is used in production for critical stuff in some
medium internet corporations. But all use case as not
been perfectly test in the real world.

# Limitations

https://python-mysql-replication.readthedocs.org/en/latest/limitations.html

# Featured

- [Data Pipelines Pocket Reference](https://www.oreilly.com/library/view/data-pipelines-pocket/9781492087823/) (by James Densmore, O'Reilly): Introduced and exemplified in Chapter 4: Data Ingestion: Extracting Data.
- [Streaming Changes in a Database with Amazon Kinesis](https://aws.amazon.com/blogs/database/streaming-changes-in-a-database-with-amazon-kinesis/) (by Emmanuel Espina, Amazon Web Services)
- [Near Zero Downtime Migration from MySQL to DynamoDB](https://aws.amazon.com/ko/blogs/big-data/near-zero-downtime-migration-from-mysql-to-dynamodb/) (by YongSeong Lee, Amazon Web Services)
- [Enable change data capture on Amazon RDS for MySQL applications that are using XA transactions](https://aws.amazon.com/ko/blogs/database/enable-change-data-capture-on-amazon-rds-for-mysql-applications-that-are-using-xa-transactions/) (by Baruch Assif, Amazon Web Services)
- [Reading MySQL Binlog with Python](https://medium.com/@mahadir.ahmad/reading-mysql-binlog-with-python-c85a8ece3b28)
- [Using python-mysql-replication with Dolt](https://www.dolthub.com/blog/2024-08-08-python-mysql-replication-works-with-dolt/)

# Projects using this library

- pg_chameleon: Migration and replica from MySQL to PostgreSQL https://github.com/the4thdoctor/pg_chameleon
- Yelp Data Pipeline: https://engineeringblog.yelp.com/2016/11/open-sourcing-yelps-data-pipeline.html
- Singer.io Tap for MySQL (https://github.com/singer-io/tap-mysql)
- MySQL River Plugin for ElasticSearch: https://github.com/scharron/elasticsearch-river-mysql
- Ditto: MySQL to MemSQL replicator https://github.com/memsql/ditto
- ElasticMage: Full Magento integration with ElasticSearch https://github.com/ElasticMage/elasticmage
- Cache buster: an automatic cache invalidation system https://github.com/rackerlabs/cache-busters
- Zabbix collector for OpenTSDB https://github.com/OpenTSDB/tcollector/blob/master/collectors/0/zabbix_bridge.py
- Meepo: Event sourcing and event broadcasting for databases. https://github.com/eleme/meepo
- Python MySQL Replication Blinker: This package read events from MySQL binlog and send to blinker's signal. https://github.com/tarzanjw/python-mysql-replication-blinker
- aiomysql_replication: Fork supporting asyncio https://github.com/jettify/aiomysql_replication
- python-mysql-eventprocessor: Daemon interface for handling MySQL binary log events. https://github.com/jffifa/python-mysql-eventprocessor
- mymongo: MySQL to mongo replication https://github.com/njordr/mymongo
- pg_ninja: The ninja elephant obfuscation and replica tool https://github.com/transferwise/pg_ninja/ (http://tech.transferwise.com/pg_ninja-replica-with-obfuscation/)
- MySQLStreamer: MySQLStreamer is a database change data capture and publish system https://github.com/Yelp/mysql_streamer
- binlog2sql: a popular binlog parser that could convert raw binlog to sql and also could generate flashback sql from raw binlog (https://github.com/danfengcao/binlog2sql)
- Streaming mysql binlog replication to Snowflake/Redshift/BigQuery (https://github.com/trainingrocket/mysql-binlog-replication)
- MySQL to Kafka (https://github.com/scottpersinger/mysql-to-kafka/)
- Aventri MySQL Monitor (https://github.com/aventri/mysql-monitor)
- BitSwanPump: A real-time stream processor (https://github.com/LibertyAces/BitSwanPump)
- clickhouse-mysql-data-reader: https://github.com/Altinity/clickhouse-mysql-data-reader
- py-mysql-elasticsearch-sync: https://github.com/jaehyeonpy/py-mysql-elasticsearch-sync
- synch: Sync data from other DB to ClickHouse (https://github.com/long2ice/synch)
- sb-osc: Online schema change tool created by Sendbird (https://github.com/sendbird/sb-osc)

# MySQL server settings

In your MySQL server configuration file you need to enable replication:

    [mysqld]
    server-id		           = 1
    log_bin			           = /var/log/mysql/mysql-bin.log
    binlog_expire_logs_seconds = 864000
    max_binlog_size            = 100M
    binlog-format              = ROW #Very important if you want to receive write, update and delete row events
    binlog_row_metadata        = FULL
    binlog_row_image           = FULL

reference: https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html

# Examples

All examples are available in the [examples directory](https://github.com/noplay/python-mysql-replication/tree/main/examples)

This example will dump all replication events to the console:

```python
from pymysqlreplication import BinLogStreamReader

mysql_settings = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': ''}

stream = BinLogStreamReader(connection_settings = mysql_settings, server_id=100)

for binlogevent in stream:
    binlogevent.dump()

stream.close()
```

For this SQL sessions:

```sql
CREATE DATABASE test;
use test;
CREATE TABLE test4 (id int NOT NULL AUTO_INCREMENT, data VARCHAR(255), data2 VARCHAR(255), PRIMARY KEY(id));
INSERT INTO test4 (data,data2) VALUES ("Hello", "World");
UPDATE test4 SET data = "World", data2="Hello" WHERE id = 1;
DELETE FROM test4 WHERE id = 1;
```

Output will be:

    === RotateEvent ===
    Date: 1970-01-01T01:00:00
    Event size: 24
    Read bytes: 0

    === FormatDescriptionEvent ===
    Date: 2012-10-07T15:03:06
    Event size: 84
    Read bytes: 0

    === QueryEvent ===
    Date: 2012-10-07T15:03:16
    Event size: 64
    Read bytes: 64
    Schema: test
    Execution time: 0
    Query: CREATE DATABASE test

    === QueryEvent ===
    Date: 2012-10-07T15:03:16
    Event size: 151
    Read bytes: 151
    Schema: test
    Execution time: 0
    Query: CREATE TABLE test4 (id int NOT NULL AUTO_INCREMENT, data VARCHAR(255), data2 VARCHAR(255), PRIMARY KEY(id))

    === QueryEvent ===
    Date: 2012-10-07T15:03:16
    Event size: 49
    Read bytes: 49
    Schema: test
    Execution time: 0
    Query: BEGIN

    === TableMapEvent ===
    Date: 2012-10-07T15:03:16
    Event size: 31
    Read bytes: 30
    Table id: 781
    Schema: test
    Table: test4
    Columns: 3

    === WriteRowsEvent ===
    Date: 2012-10-07T15:03:16
    Event size: 27
    Read bytes: 10
    Table: test.test4
    Affected columns: 3
    Changed rows: 1
    Values:
    --
    * data : Hello
    * id : 1
    * data2 : World

    === XidEvent ===
    Date: 2012-10-07T15:03:16
    Event size: 8
    Read bytes: 8
    Transaction ID: 14097

    === QueryEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 49
    Read bytes: 49
    Schema: test
    Execution time: 0
    Query: BEGIN

    === TableMapEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 31
    Read bytes: 30
    Table id: 781
    Schema: test
    Table: test4
    Columns: 3

    === UpdateRowsEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 45
    Read bytes: 11
    Table: test.test4
    Affected columns: 3
    Changed rows: 1
    Affected columns: 3
    Values:
    --
    * data : Hello => World
    * id : 1 => 1
    * data2 : World => Hello

    === XidEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 8
    Read bytes: 8
    Transaction ID: 14098

    === QueryEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 49
    Read bytes: 49
    Schema: test
    Execution time: 1
    Query: BEGIN

    === TableMapEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 31
    Read bytes: 30
    Table id: 781
    Schema: test
    Table: test4
    Columns: 3

    === DeleteRowsEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 27
    Read bytes: 10
    Table: test.test4
    Affected columns: 3
    Changed rows: 1
    Values:
    --
    * data : World
    * id : 1
    * data2 : Hello

    === XidEvent ===
    Date: 2012-10-07T15:03:17
    Event size: 8
    Read bytes: 8
    Transaction ID: 14099

# Tests

When it's possible we have a unit test.

More information is available here:
https://python-mysql-replication.readthedocs.org/en/latest/developement.html

# Changelog

https://github.com/julien-duponchelle/python-mysql-replication/blob/main/CHANGELOG

# Similar projects

- Kodoma: Ruby-binlog based MySQL replication listener https://github.com/y310/kodama
- MySQL Hadoop Applier: C++ version http://dev.mysql.com/tech-resources/articles/mysql-hadoop-applier.html
- Java: https://github.com/shyiko/mysql-binlog-connector-java
- GO: https://github.com/siddontang/go-mysql
- PHP: Based on this this project https://github.com/krowinski/php-mysql-replication and https://github.com/fengxiangyun/mysql-replication
- .NET: https://github.com/SciSharp/dotnet-mysql-replication
- .NET Core: https://github.com/rusuly/MySqlCdc

# Special thanks

- MySQL binlog from Jeremy Cole was a great source of knowledge about MySQL replication protocol https://github.com/jeremycole/mysql_binlog
- Samuel Charron for his help https://github.com/scharron

# Contributors

Major contributor:

- Julien Duponchelle Original author https://github.com/noplay
- bjoernhaeuser for his bugs fixing, improvements and community support https://github.com/bjoernhaeuser
- Arthur Gautier gtid, slave report... https://github.com/baloo

Maintainer:

- Julien Duponchelle Original author https://github.com/noplay
- Sean-k1 https://github.com/sean-k1
- dongwook-chan https://github.com/dongwook-chan

Other contributors:

- Dvir Volk for bug fix https://github.com/dvirsky
- Lior Sion code cleanup and improvements https://github.com/liorsion
- Lx Yu code improvements, primary keys detections https://github.com/lxyu
- Young King for pymysql 0.6 support https://github.com/youngking
- David Reid checksum checking fix https://github.com/dreid
- Alex Gaynor fix smallint24 https://github.com/alex
- lifei NotImplementedEvent https://github.com/lifei
- Maralla Python 3.4 fix https://github.com/maralla
- Daniel Gavrila more MySQL error codes https://github.com/danielduduta
- Bernardo Sulzbach code cleanup https://github.com/mafagafogigante
- Darioush Jalali Python 2.6 backport https://github.com/darioush
- Jasonz bug fixes https://github.com/jasonzzz
- Bartek Ogryczak cleanup and improvements https://github.com/vartec
- Wang, Xiaozhe cleanup https://github.com/chaoslawful
- siddontang improvements https://github.com/siddontang
- Cheng Chen Python 2.6 compatibility https://github.com/cccc1999
- Jffifa utf8mb4 compatibility https://github.com/jffifa
- Romuald Brunet bug fixes https://github.com/romuald
- Cédric Hourcade Don't fail on incomplete dates https://github.com/hc
- Giacomo Lozito Explicit close stream connection on exception https://github.com/giacomolozito
- Giovanni F. MySQL 5.7 support https://github.com/26fe
- Igor Mastak intvar event https://github.com/mastak
- Xie Zhenye fix missing update \_next_seq_no https://github.com/xiezhenye
- Abrar Sheikh: Multiple contributions https://github.com/abrarsheikh
- Keegan Parker: secondary database for reference schema https://github.com/kdparker
- Troy J. Farrell Clear table_map if RotateEvent has timestamp of 0 https://github.com/troyjfarrell
- Zhanwei Wang Fail to get table informations https://github.com/wangzw
- Alexander Ignatov Fix the JSON literal
- Garen Chan Support PyMysql with a version greater than 0.9.3 https://github.com/garenchan
- Mike Ascah: Add logic to handle inlined ints in large json documents ttps://github.com/mascah
- Hiroaki Kawai: PyMySQL 1.0 support (https://github.com/hkwi)
- Dongwook Chan: Support for ZEROFILL, Correct timedelta value for negative MySQL TIME datatype, Fix parsing of row events for MySQL8 partitioned table, Parse status variables in query event, Parse status variables in query event , Fix parse errors with MariaDB (https://github.com/dongwook-chan)
- Paul Vickers: Add support for specifying an end log_pos (https://github.com/paulvic)
- Samira El Aabidi: Add support for MariaDB GTID (https://github.com/Samira-El)
- Oliver Seemann: Handle large json, github actions,
  Zero-pad fixed-length binary fields (https://github.com/oseemann)
- Mahadir Ahmad: Handle null json payload (https://github.com/mahadirz)
- Axel Viala: Removal of Python 2.7 (https://github.com/darnuria)
- Etern: Add XAPrepareEvent, parse last_committed & sequence_number of GtidEvent (https://github.com/etern)
- Jason Fulghum: typo in ident variable name (https://github.com/fulghum)

Thanks to GetResponse for their support

# Licence

Copyright 2012-2024 Julien Duponchelle

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
