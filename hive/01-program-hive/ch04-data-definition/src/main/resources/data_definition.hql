-- 删除数据库
DROP TABLE IF EXISTS financials;
-- 默认情况下， Hive 是不允许用户删除一个包含有表的数据库的。用户要么先删除数据
-- 库中的表，然后再删除数据库，要么在删除命令的最后面加上关键字CASCADE, 这样
-- 可以使Hive 自行先删除数据库中的表
DROP DATABASE IF EXISTS financials CASCADE;

-- 创建数据库
CREATE DATABASE financials
    COMMENT 'Holds all financials'
    LOCATION '/my/preferred/directory'
    WITH DBPROPERTIES ('creator' = 'Mark Moneybags','date' = '2012-01-02');;;

-- 描述表
DESCRIBE DATABASE financials;
DESCRIBE DATABASE EXTENDED financials;

-- 修改表
ALTER DATABASE financials SET DBPROPERTIES ('edited-by' = 'Joe Dba');

-- 创建表
CREATE TABLE IF NOT EXISTS mydb.employees
(
    name         STRING COMMENT 'Employee name',
    salary       FLOAT COMMENT 'Employee salary',
    subordinates ARRAY<STRING> COMMENT 'Names of subordinates',
    deductions   MAP<STRING, FLOAT>COMMENT 'Keys are deductions names, values are percentages',
    address      STRUCT<street: STRING, city: STRING, state: STRING, zip:INT>COMMENT 'Home address'
)
    COMMENT 'Description of the table'
    TBLPROPERTIES ('creator' = 'me','created at' = '2012-01-02 10:00:00');
--     LOCATION '/user/hive/warehouse/mydb.db/employees';

DESCRIBE EXTENDED mydb.employees;

-- exchange是关键字需要使用引号
-- 因为表是外部的，所以Hive 并非认为其完全拥有这份数据。因此，删除该表并不会删
-- 除掉这份数据，不过描述表的元数据信息会被删除掉。
CREATE EXTERNAL TABLE IF NOT EXISTS stocks
(
    `exchange`      STRING,
    symbol          STRING,
    ymd             STRING,
    price_open      FLOAT,
    price_high      FLOAT,
    price_low       FLOAT,
    price_close     FLOAT,
    volume          INT,
    price_adj_close FLOAT
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION '/data/stocks';

-- 对于管理表，用户还可以对一张存在的表进行表结构复制（而不会复制数据）
-- 如果语句中省略掉EXTERNAL 关键字而且源表是外部表的话，那
-- 么生成的新表也将是外部表。如果语句中省略掉EXTERNAL 关键字而且
-- 源表是管理表的话，那么生成的新表也将是管理表。但是，如果语句中
-- 包含有EXTERNAL 关键字而且源表是管理表的话，那么生成的新表将是
-- 外部表。即使在这种场景下， LOCATION 子句同样是可选的。
CREATE EXTERNAL TABLE IF NOT EXISTS mydb.employees3
    LIKE mydb.employees
        LOCATION '/path/to/data';

CREATE TABLE employees
(
    name         STRING,
    salary       FLOAT,
    subordinates ARRAY<STRING>,
    deductions   MAP<STRING, FLOAT>,
    address      STRUCT<street: STRING, city:STRING, state: STRING, zip: INT>
) PARTITIONED BY (country STRING, state STRING);

-- 向表中导入数据
LOAD DATA INPATH '/data/employees/employees.txt' INTO TABLE employees;

SELECT*
FROM employees
WHERE country = 'US'
  AND state = 'IL';

-- 将Hive设置为"strict(严格）“模式，这样如果对分区表进行查询而WHERE 子句没有加分区过
-- 滤的话，将会禁止提交这个任务。
set hive.mapred.mode=strict;
SELECT e.name, e.salary
FROM employees e
LIMIT 100;

set hive.mapred.mode=nonstrict;
SELECT e.name, e.salary
FROM employees e
LIMIT 100;

-- 通过SHOW PARTITIONS 命令查看表中存在的所有分区：
SHOW PARTITIONS employees;

-- 可以在这个命令上增加一个指定了一个或者多个特定分区字段值的PARTITION
-- 子句，进行过滤查询：
SHOW PARTITIONS employees PARTITION (country = 'US');

DESCRIBE EXTENDED employees;

CREATE EXTERNAL TABLE IF NOT EXISTS log_messages
(
    hms        INT,
    severity   STRING,
    server     STRING,
    process_id INT,
    message    STRING
)
    PARTITIONED BY (year INT, month INT, day INT)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';


ALTER TABLE log_messages
    ADD PARTITION (year = 2012, month = 1, day = 2)
        LOCATION 'hdfs://master_server/data/log_messages/2012/01/02';

DESCRIBE EXTENDED log_messages;

DESCRIBE EXTENDED log_messages PARTITION (year = 2012, month = 1,day = 2);

CREATE TABLE kst
    PARTITIONED BY (ds string)
    ROW FORMAT SERDE 'com.linkedin.haivvreo.AvroSerDe'
        WITH SERDEPROPERTIES ('schema.url' = 'http://schema_provider/kst.avsc')
    STORED AS
        INPUTFORMAT 'com.linkedin.haivvreo.AvroContainerinputFormat'
        OUTPUTFORMAT 'com.linkedin.haivvreo.AvroContainerOutputFormat';

CREATE EXTERNAL TABLE IF NOT EXISTS stocks
(
    `exchange`      STRING,
    symbol          STRING,
    ymd             STRING,
    price_open      FLOAT,
    price_high      FLOAT,
    price_low       FLOAT,
    price_close     FLOAT,
    volume          INT,
    price_adj_close FLOAT
)
    CLUSTERED BY (`exchange`, symbol)
        SORTED BY (ymd ASC)
        INTO 96 BUCKETS
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION '/data/stocks';

-- 表重命名
ALTER TABLE log_messages
    RENAME TO logmsgs;

ALTER TABLE log_messages
    ADD IF NOT EXISTS
        PARTITION (year = 2011, month = 1, day = 1) LOCATION '/logs/2011/01/01'
        PARTITION (year = 2011, month = 1, day = 2) LOCATION '/logs/2011/01/02'
        PARTITION (year = 2011, month = 1, day = 3) LOCATION '/logs/2011/01/03';

ALTER TABLE log_messages
    PARTITION (year = 2011, month = 12, day = 2)
        SET LOCATION 's3n://ourbucket/logs/2011/01/02';

ALTER TABLE log_messages
    DROP IF EXISTS PARTITION (year = 2011, month = 12, day = 2);


-- 修改列信息
-- 如果用户想将这个字段移动到第一个位置，那么只需要使用FIRST 关键字替代AFTER other_ column 子句即可。
ALTER TABLE log_messages
    CHANGE COLUMN hms hours_minutes_seconds INT
        COMMENT 'The hours, minutes, and seconds part of the timestamp'
        AFTER severity;

-- 增加列
ALTER TABLE log_messages
    ADD COLUMNS (
        app_name STRING COMMENT 'Application name',
        session_id BIGINT COMMENT 'The current session id');

-- 删除或者替换列
-- REPLACE 语句只能用于使用了如下2 种内置SerDe 模块的表： DynamicSerDe 或者
-- MetadataTypedColumnsetSerDe
ALTER TABLE log_messages
    REPLACE COLUMNS (
        hours_mins_secs INT COMMENT 'hour, minute, seconds from timestamp',
        severity STRING COMMENT 'The message severity',
        message STRING COMMENT 'The rest of the message');

-- 修改表属性
-- 用户可以增加附加的表属性或者修改已经存在的属性，但是无法删除属性：
ALTER TABLE log_messages
    SET TBLPROPERTIES (
        'notes' = 'The process id is no longer captured; this column is always NULL');

-- 修改存储属性

ALTER TABLE log_messages
    PARTITION (year = 2012, month = 1, day = 1)
        SET FILEFORMAT SEQUENCEFILE;

CREATE TABLE table_using_json_storage;
ALTER TABLE table_using_json_storage
    SET SERDE 'com.example.JSONSerDe'
        WITH SERDEPROPERTIES ('propl' = 'valuel', 'prop2' = 'value2');

ALTER TABLE stocks
    CLUSTERED BY (`exchange`, symbol)
        SORTED BY (symbol)
        INTO 48 BUCKETS;

ALTER TABLE log_messages
    TOUCH PARTITION (year = 2012, month = 1, day = 1);

-- ALTER TABLE … ARCHIVE PARTITION 语句会将这个分区内的文件打成一个
-- Hadoop 压缩包(HAR) 文件。但是这样仅仅可以降低文件系统中的文件数以及减轻
-- NameNode 的压力，而不会减少任何的存储空间（例如，通过压缩）
-- 使用UNARCHIVE 替换ARCHIVE 就可以反向操作。这个功能只能用于分区表中独立
-- 的分区。
ALTER TABLE log_messages
    ARCHIVE
        PARTITION (year = 2012, month = 1, day = 1);

ALTER TABLE log_messages
    PARTITION (year = 2012, month = 1, day = 1) ENABLE NO_DROP;
ALTER TABLE log_messages
    PARTITION (year = 2012, month = 1, day = 1) ENABLE OFFLINE;






