DROP TABLE IF EXISTS text;
CREATE TABLE text
(
    x INT
);
DESCRIBE EXTENDED text;

DROP TABLE IF EXISTS seq;
CREATE TABLE seq
(
    x INT
) STORED AS SEQUENCEFILE;
DESCRIBE EXTENDED seq;

DESCRIBE a;
SELECT *
FROM a;

create table column_table
(
    key   int,
    value int
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
    STORED AS
        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';

INSERT OVERWRITE TABLE column_table
SELECT a.a, a.b
FROM a;

-- SerDe
CREATE TABLE serde_regex
(
    host     STRING,
    identity STRING,
    `user`   STRING,
    `time`   STRING,
    request  STRING,
    status   STRING,
    size     STRING,
    referer  STRING,
    agent    STRING
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
        WITH SERDEPROPERTIES (
        "input.regex" =
                "([^ ]*) ([^)*) ([^ ]*) (-|\\[[^ ]]*\\])([^ \"] *|\" [^\"] *\") (-|[0-9]*) (-|[0-9]*) (?: ([^ \"]*|\"[^\"]*\")([^ \"]*|\"[^\"]*\"))?",
        "output.format.string" = " %1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s")
    STORED AS TEXTFILE;

SELECT xpath('<a><b id="foo">bl</b><b id="bar">b2</b></a>', '//@id')
FROM src
LIMIT 1;

SELECT xpath('<a><b class="bb">bl</b><b>b2</b><b>b3</b><c class="bb">cl</c><c>c2</c></a>', 'a/*[@class="bb"]/text()')
FROM src
LIMIT 1;

-- JSON SerDe
DROP TABLE IF EXISTS messages;
CREATE EXTERNAL TABLE messages
(
    id         STRING,
    created_at STRING,
    text       STRING,
    `user`     MAP<STRING, STRING>
)
    ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.JsonSerDe";
--         WITH SERDEPROPERTIES (
--         "msg_id" = "$.id",
--         "tstamp" = "$.created_at",
--         "text" = "$.text",
--         "user_id" = "$.user.id",
--         "user_name" = "$.user.name");

LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch15-file-format/src/main/resources/messages_table_data.json"
    INTO TABLE messages;
SELECT *
FROM messages;

-- DROP TABLE IF EXISTS messages2;
-- CREATE EXTERNAL TABLE messages2
-- (
--     msg_id    BIGINT,
--     tstamp    STRING,
--     text      STRING,
--     user_id   BIGINT,
--     user_name STRING
-- )
--     ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.JsonSerDe"
--         WITH SERDEPROPERTIES (
--         "msg_id" = "$.id",
--         "tstamp" = "$.created_at",
--         "text" = "$.text",
--         "user_id" = "$.user.id",
--         "user_name" = "$.user.name");
--
-- LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch15-file-format/src/main/resources/messages_table_data.json"
--     INTO TABLE messages2;
-- SELECT *
-- FROM messages2;


-- 使用表属性信息定义Avro Schema
CREATE TABLE doctors
    ROW FORMAT
        SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    TBLPROPERTIES ('avro. schema.literal' = '{
                "namespace": "testing.hive.avro.serde",
                "name": "doctors",
                "type": "record",
                "fields": [{
                    "name": "number",
                    "type":"int",
                    "doc":"Order of playing the role"
                },{
                    "name":"first_name",
                    "type":"string",
                    "doc":"first name of actor playing role"
                },{
                    "name":"last_name",
                    "type":"string",
                    "doc":"last name of actor playing role"
                }]
            }');






















