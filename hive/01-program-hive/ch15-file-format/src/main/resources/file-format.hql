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

SELECT xpath('<a><b id="foo">bl</b><b id="bar">b2</b></a>', '//@id') FROM src LIMIT 1;

SELECT xpath ('<a><b class="bb">bl</b><b>b2</b><b>b3</b><c class="bb">cl</c><c>c2</c></a>', 'a/*[@class="bb"]/text()') FROM src LIMIT 1;

