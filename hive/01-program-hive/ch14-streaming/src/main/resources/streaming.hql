SELECT * FROM a;

-- 恒等变换
SELECT TRANSFORM (a, b)
USING 'cat' AS newA, newB
FROM a;

-- 改变类型
SELECT TRANSFORM (a, b)
USING 'cat' AS (newA INT, newB DOUBLE)
FROM a;


SELECT TRANSFORM (a, b)
USING 'cut -f1' AS newA, newB
FROM a;

-- 操作转换
SELECT TRANSFORM (a, b)
USING 'sed s/4/10/' AS newA, newB
FROM a;

DELETE FILE /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/ctof.sh;
ADD FILE /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/ctof.sh;
SELECT * FROM a;
SELECT TRANSFORM (a) USING 'ctof.sh' AS convert
FROM a;

DROP TABLE IF EXISTS kv_data;
CREATE TABLE kv_data (line STRING);
LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/kv_data.txt"
INTO TABLE kv_data;
SELECT * FROM kv_data;

DELETE FILE /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/split_kv.py;
ADD FILE /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/split_kv.py;

SELECT TRANSFORM (line)
USING'python split_kv.py'
AS (key, value) FROM kv_data;

-- 使用streaming 进行聚合计算
CREATE TABLE sum (number INT);
LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/sum_table_data.txt"
INTO TABLE sum;
SELECT * FROM sum;

DELETE FILE /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/sum.py;
ADD FILE /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/sum.py;

SELECT TRANSFORM (number) USING'python sum.py'AS total FROM sum;

-- CLUSTER BY 、DISTRIBUTE BY 、SORT BY
DROP TABLE IF EXISTS docs;
CREATE TABLE docs (line STRING)
ROW FORMAT DELIMITED
    LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/docs_table_data.txt"
INTO TABLE docs;
SELECT * FROM docs LIMIT 10;

DROP TABLE IF EXISTS word_count;
CREATE TABLE word_count
(
    word  STRING,
    count INT
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
SELECT * FROM
              word_count;
SELECT TRANSFORM (line)
USING 'python /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/word_mapper.py'
AS word, count
FROM docs LIMIT 20;

-- 还有问题
FROM (
         FROM docs
         SELECT TRANSFORM (line)
         USING 'python /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/word_mapper.py'
         AS(word, count)
         CLUSTER BY word) we
INSERT
OVERWRITE
TABLE
word_count
SELECT TRANSFORM (we.word, we.count)
    USING 'python /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch14-streaming/src/main/resources/word_reducer.py'
    AS word, count;