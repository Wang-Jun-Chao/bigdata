hive -e "set io.compression.codecs"

-- 开启中间压缩
-- <property>
--     <name>hive.exec.compress.intermediate</name>
--     <value>true</value>
--     <description> This controls whether intermediate files produced by Hive between
--         multiple map-reduce jobs are compressed. The compression codec and other
--         options are de termined from hadoop config variables mapred.output.compress*
--     </description>
-- </property>

-- 最终输出结果压缩
-- 如果属性hive.exec.compress.output 的值设置为true, 那么这时需要为其指定一个编解
-- 码器。对于输出文件，使用GZip 进行压缩是个不错的主意，因为其通常可以大幅度降
-- 低文件的大小。但是，需要记住的是GZip 压缩的文件对千后面的MapReduce job 而言
-- 是不可分割的。
-- <property>
--     <name>hive.exec.compress.outpu 七</name>
--     <value>false</value>
--     <description>
--         This controls whether the final outputs of a query
--         (to a local/hdfs file or a Hive table) is compressed. The compression
--         codec and other options are determined from hadoop config variables
--         mapred.ou 七put.compress*
--     </description>
-- </property>

-- <property>
--     <name>mapred.output.compression.codec</name>
--     <value>org.apache.hadoop.io.compress.GzipCodec</value>
--     <description>
--         If the job outputs are compressed, how should they be
--         compressed?
--     </description>
-- </property>

-- sequence file 存储格式
CREATE TABLE a_sequence_file_table(data STRING) STORED AS SEQUENCEFILE;
CREATE TABLE a
(
    a INT,
    b INT
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch11-compression/src/main/resources/a_table_data.txt"
INTO TABLE a;

set hive.exec.compress.intermediate=true;
CREATE TABLE intermediate_comp_on
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT *
FROM a;

set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GZipCodec;
set hive.exec.compress.intermediate=true;
CREATE TABLE intermediate_comp_on_gz
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT *
FROM a;

set hive.exec.compress.output=true;
CREATE TABLE final_comp_on
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT *
FROM a;


set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
CREATE TABLE final_comp_on_gz
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT *
FROM a;
SELECT * FROM final_comp_on_gz;

set mapred.output.compression.type=BLOCK;
set hive.exec.compress.output= true;
set hive.exec.compress.intermediate=false;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
CREATE TABLE final_comp_on_gz_seq
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS SEQUENCEFILE
AS
SELECT *
FROM a;

SELECT * FROM final_comp_on_gz_seq;

set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set hive.exec.compress.intermediate=true;
set mapred.output.compression.type=BLOCK;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

CREATE TABLE final_comp_on_gz_int_compress_snappy_seq
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS SEQUENCEFILE AS
SELECT *
FROM a;

-- 存档分区
CREATE TABLE hive_text
(
    line STRING
) PARTITIONED BY (folder STRING);
ALTER TABLE hive_text ADD PARTITION (folder='docs');

LOAD DATA LOCAL INPATH '/Users/wangjunchao/Software/apache-hive-3.1.1-bin/RELEASE_NOTES.txt'
    INTO TABLE hive_text PARTITION (folder = 'docs');
LOAD DATA LOCAL INPATH '/Users/wangjunchao/Software/apache-hive-3.1.1-bin/LICENSE'
    INTO TABLE hive_text PARTITION (folder = 'docs');

SELECT *
FROM hive_text
WHERE line LIKE '%hive%'
LIMIT 2;



dfs -ls /user/hive/warehouse/mydb.db/hive_text/folder=docs;

SET hive.archive.enabled=true;
set hive.archive.har.parentdir.settable=true;
set har.partfile.size=1099511627776;

ALTER TABLE hive_text
    ARCHIVE PARTITION (folder = 'docs');

ALTER TABLE hive_text
    UNARCHIVE PARTITION (folder = 'docs');





