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