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

ADD JAR /Users/wangjunchao/Project/bigdata/hive/01-program-hive/ch13-function/target/ch13-function-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION zodiac AS 'wjc.bigdata.hive.ch13function.UDFZodiacSign';

DESCRIBE FUNCTION zodiac;
DESCRIBE FUNCTION EXTENDED zodiac;

SELECT name, bday, zodiac(bday) FROM littlebigdata;

DROP TEMPORARY FUNCTION IF EXISTS zodiac;







