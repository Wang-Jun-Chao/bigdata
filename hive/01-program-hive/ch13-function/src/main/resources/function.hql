DESCRIBE FUNCTION concat;
DESCRIBE FUNCTION EXTENDED concat;

SELECT avg(price_close)
FROM stocks
WHERE `exchange` ='NASDAQ' AND symbol='AAPL';

SELECT year(ymd), avg(price_close)
FROM stocks
WHERE `exchange` = 'NASDAQ'
  AND symbol = 'AAPL'
GROUP BY year(ymd);

CREATE TABLE dual(a INT);
SELECT array(1,2,3) FROM dual;
SELECT explode(array(1,2,3)) AS element FROM src;
-- ERROR
SELECT name, explode(subordinates) FROM employees;

SELECT name, sub
FROM employees
         LATERAL VIEW explode(subordinates) subView AS sub;

DROP TABLE littlebigdata;
CREATE TABLE IF NOT EXISTS littlebigdata
(
    name   STRING,
    email  STRING,
    bday   STRING,
    ip     STRING,
    gender STRING,
    anum   INT
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/src/main/resources/littlebigdata.txt'
    INTO TABLE littlebigdata;

-- 如果发现找不要指定类，可以先删除jar再添加
DELETE JAR /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/target/ch13-function-1.0-SNAPSHOT.jar;
ADD JAR /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/target/ch13-function-1.0-SNAPSHOT.jar;

CREATE TEMPORARY FUNCTION zodiac AS 'wjc.bigdata.hive.ch13function.UDFZodiacSign';

DESCRIBE FUNCTION zodiac;
DESCRIBE FUNCTION EXTENDED zodiac;

SELECT name, bday, zodiac(bday) FROM littlebigdata;

DROP TEMPORARY FUNCTION IF EXISTS zodiac;

CREATE TEMPORARY FUNCTION nvl AS 'wjc.bigdata.hive.ch13function.GenericUDFNvl';


LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch10-turning/src/main/resources/onecol_table_data.txt"
INTO TABLE src;

SELECT nvl(1, 2)          AS COLl,
       nvl(NULL, 5)       AS COL2,
       nvl(NULL, "STUFF") AS COL3
FROM src
LIMIT 1;

DROP TABLE IF EXISTS people;
CREATE TABLE people
(
    name       STRING,
    friendname STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";
LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/src/main/resources/people_table_data.txt"
INTO TABLE people;
SELECT * FROM people;

CREATE TABLE collecttest
(
    str      STRING,
    countVal INT
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/src/main/resources/collecttest_table_data.txt"
INTO TABLE collecttest;

DELETE JAR /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/target/ch13-function-1.0-SNAPSHOT.jar;
ADD JAR /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/target/ch13-function-1.0-SNAPSHOT.jar;

CREATE TEMPORARY FUNCTION collect AS 'wjc.bigdata.hive.ch13function.GenericUDAFCollect';
SELECT collect(str) FROM collecttest;

SELECT str, concat_ws(',', collect(cast(countVal AS STRING))) FROM collecttest GROUP BY str;

CREATE TEMPORARY FUNCTION forx AS 'wjc.bigdata.hive.ch13function.GenericUDTFFor';
SELECT forx(1, 5) AS i FROM collecttest;

DROP TABLE IF EXISTS weblogs;
CREATE TABLE weblogs
(
    url STRING
);

LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/src/main/resources/weblogs_table_data.txt"
INTO TABLE weblogs;
SELECT * FROM weblogs;


SELECT parse_url_tuple(weblogs.url, 'HOST', 'PATH') AS (host, path)
FROM weblogs;

DROP TABLE IF EXISTS books;
CREATE TABLE books (info String);
LOAD DATA LOCAL INPATH "/Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/src/main/resources/books_table_data.txt"
INTO TABLE books;
SELECT * FROM books;

SELECT cast(split (info, "\\|") [0] AS INTEGER) AS isbn FROM books
    WHERE split(info,"\\|") [1] = "Programming Hive";

DELETE JAR /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/target/ch13-function-1.0-SNAPSHOT.jar;
ADD JAR /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/target/ch13-function-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION parse_book AS "wjc.bigdata.hive.ch13function.UDTFBook";

SELECT parse_book(info) AS (isbn, title, authors) FROM books;
FROM (
        SELECT parse_book(info) AS (isbn, title, authors) FROM books) a
SELECT a.isbn
WHERE a.title = "Programming Hive"
  AND array_contains(authors, 'Edward');

-- 宏命令
CREATE TEMPORARY MACRO SIGMOID (x DOUBLE) 1.0 / (1.0 + EXP(-x));
SELECT SIGMOID(2) FROM src LIMIT 1;









