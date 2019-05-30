SELECT name, salary
FROM employees;

SELECT name, subordinates
FROM employees;

SELECT name, deductions
FROM employees;

SELECT name, address
FROM employees;

SELECT name, subordinates[0]
FROM employees;

SELECT name, deductions["State Taxes"]
FROM employees;

SELECT name, address.city
FROM employees;

SET hive.support.quoted.identifiers=none;
SELECT symbol, `price.*`
FROM stocks
LIMIT 10;

SELECT upper(name),
       salary,
       deductions["Federal Taxes"],
       round(salary * (1 - deductions["Federal Taxes"]))
FROM employees;

-- 可以通过设置属性hive.map.aggr 值为true 来提高聚合的性能
SET hive.map.aggr=true;
SELECT count(*), avg(salary)
FROM employees;
SELECT count(DISTINCT symbol)
FROM stocks;

SELECT count(DISTINCT ymd), count(DISTINCT volume)
FROM stocks;

-- 表生成函数:其可以将单列扩展成多列或者多行。当使用表生成函数时，
-- Hive 要求使用列别名。
SELECT explode(subordinates) AS sub
FROM employees;

SELECT parse_url_tuple(url, 'HOST', 'PATH', 'QUERY')
           as (host, path, query)
FROM url_table;

SELECT upper(name),
       salary,
       deductions["Federal Taxes"],
       round(salary * (1 - deductions["Federal Taxes"]))
FROM employees
LIMIT 2;

-- 列别名
SET hive.cli.print.header=true;
SELECT upper(name),
       salary,
       deductions["Federal Taxes"]                       as fed_taxes,
       round(salary * (1 - deductions["Federal Taxes"])) as salary_minus_fed_taxes
FROM employees
LIMIT 2;

-- 嵌套SELECT 语句
FROM (
         SELECT upper(name)                                       as name,
                salary,
                deductions["Federal Taxes"]                       as fed_taxes,
                round(salary * (1 - deductions["Federal Taxes"])) as salary_minus_fed_taxes
         FROM employees
     ) e
SELECT e.name, e.salary_minus_fed_taxes
WHERE e.salary_minus_fed_taxes > 70000;

-- CASE … WHEN … THEN 句式
SELECT name,
       salary,
       CASE
           WHEN salary < 50000.0 THEN 'low'
           WHEN salary >= 50000.0 AND salary < 70000.0 THEN 'middle'
           WHEN salary >= 70000.0 AND salary < 100000.0 THEN 'high'
           ELSE 'very high'
           END AS bracket
FROM employees;

-- 什么情况下Hive 可以避免进行MapReduce:
-- 一、本地模式
-- 二、对于WHERE 语句中过滤条件只是分区字段这种情况（无论是否使用LIMIT 语句限制输出记录条数）
-- 三、如果属性hive.exec.mode.local.auto 的值设置为true 的话， Hive 还会尝试使用本地模式执行其他的操作：
SELECT*
FROM employees;
SELECT*
FROM employees
WHERE country = 'US'
  AND state = 'CA'
LIMIT 100;
set hive.exec.mode.local.auto=true;

SELECT name,
       salary,
       deductions["Federal Taxes"],
       salary * (1 - deductions["Federal Taxes"])
FROM employees
WHERE round(salary * (1 - deductions["Federal Taxes"])) > 70000;

-- ERROR
SELECT name,
       salary,
       deductions["Federal Taxes"],
       salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
FROM employees
WHERE round(salary_minus_fed_taxes) > 70000;

SELECT e.*
FROM (SELECT name,
             salary,
             deductions["Federal Taxes"]                as ded,
             salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
      FROM employees) e
WHERE round(e.salary_minus_fed_taxes) > 70000;

-- 浮点数比较
SELECT name, salary, deductions['Federal Taxes']
FROM employees
WHERE deductions['Federal Taxes'] > 0.2;

SELECT name, salary, deductions['Federal Taxes']
FROM employees
WHERE deductions['Federal Taxes'] > cast(0.2 AS FLOAT);

-- LIKE 和 RLIKE
SELECT name, address.street
FROM employees
WHERE address.street LIKE '%Ave.';

SELECT name, address.city
FROM employees
WHERE address.city LIKE 'O%';

SELECT name, address.street
FROM employees
WHERE address.street LIKE '%Chi%';

SELECT name, address.street
FROM employees
WHERE address.street RLIKE '.*(Chicago|Ontario).*';

-- GROUP BY 语句
SELECT year(ymd), avg(price_close)
FROM stocks
WHERE `exchange` = 'NASDAQ'
  AND symbol = 'AAPL'
GROUP BY year(ymd);

-- HAVING 语句
SELECT year(ymd), avg(price_close)
FROM stocks
WHERE `exchange` = 'NASDAQ'
  AND symbol = 'AAPL'
GROUP BY year(ymd)
HAVING avg(price_close) > 50.0;

SELECT a.ymd, a.price_close, b.price_close
FROM stocks a
         JOIN stocks b ON a.ymd = b.ymd
WHERE a.symbol = 'AAPL'
  AND b.symbol = 'IBM'
LIMIT 10;

-- Hive 中支持的查询语句
SELECT a.ymd, a.price_close, b.price_close
FROM stocks a
         JOIN stocks b
              ON a.ymd <= b.ymd
WHERE a.symbol = 'AAPL'
  AND b.symbol = 'IBM'
LIMIT 10;

CREATE EXTERNAL TABLE IF NOT EXISTS dividends
(
    ymd      STRING,
    dividend FLOAT
)
    PARTITIONED BY (`exchange` STRING, symbol STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM stocks s
         JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
WHERE s.symbol = 'AAPL'
LIMIT 10;

-- 当对3 个或者更多个表进行JOIN 连接时，如果每个ON 子句都使用相同
-- 的连接键的话，那么只会产生一个MapReduce job.
-- Hive 同时假定查询中最后一个表是最大的那个表。在对每行记录进行连接操作时，它
-- 会尝试将其他表缓存起来，然后扫描最后那个表进行计算。因此，用户需要保证连续
-- 查询中的表的大小从左到右是依次增加的。
SELECT a.ymd, a.price_close, b.price_close, c.price_close
FROM stocks a
         JOIN stocks b ON a.ymd = b.ymd
         JOIN stocks c ON a.ymd = c.ymd
WHERE a.symbol = 'AAPL'
  AND b.symbol = 'IBM'
  AND c.symbol = 'GE'
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM dividends d
         JOIN stocks s ON s.ymd = d.ymd AND s.symbol = d.symbol
WHERE s.symbol = 'AAPL'
LIMIT 10;

SELECT /*+STREAMTABLE (s) */ s.ymd, s.symbol, s.price_close, d.dividend
FROM stocks s
         JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
WHERE s.symbol = 'AAPL'
LIMIT 10;


SELECT s.ymd, s.symbol, s.price_close, D.dividend
FROM stocks s
         LEFT OUTER JOIN dividends d ON s.ymd = d.ymd AND s.symbol
    = d.symbol
WHERE s.symbol = 'AAPL'
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM stocks s
         LEFT OUTER JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
WHERE s.symbol = 'AAPL'
  AND s.`exchange` = 'NASDAQ'
  AND d.`exchange` = 'NASDAQ'
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM stocks s
         LEFT OUTER JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
WHERE s.symbol = 'AAPL'
  AND s.`exchange` = 'NASDAQ'
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM stocks s
         LEFT OUTER JOIN dividends d
                         ON s.ymd = d.ymd AND s.symbol = d.symbol
                             AND s.symbol = 'AAPL' AND s.`exchange` = 'NASDAQ' AND d.`exchange` = 'NASDAQ'
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM (SELECT* FROM stocks WHERE symbol = 'AAPL' AND `exchange` = 'NASDAQ') s
         LEFT OUTER JOIN
         (SELECT * FROM dividends WHERE symbol = 'AAPL' AND `exchange` = 'NASDAQ') d
         ON s.ymd = d.ymd
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM dividends d
         RIGHT OUTER JOIN stocks s ON d.ymd = s.ymd AND d.symbol = s.symbol
WHERE s.symbol = 'AAPL'
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM dividends d
         FULL OUTER JOIN stocks s ON d.ymd = s.ymd AND d.symbol = s.symbol
WHERE s.symbol = 'AAPL'
LIMIT 10;

-- LEFT SEMI-JOIN
-- 左半开连接(LEFT SEMI-JOIN) 会返回左边表的记录，前提是其记录对于右边表满足
-- ON 语句中的判定条件
-- 不支持
-- SELECT s.ymd, s.symbol, s.price_close
-- FROM stocks s
-- WHERE s.ymd, s.symbol IN
--     (SELECT d.ymd
--     , ct.symbol FROM dividends d);
SELECT s.ymd, s.symbol, s.price_close
FROM stocks s
         LEFT SEMI
         JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
LIMIT 10;

-- map-side JOIN
-- 如果所有表中只有一张表是小表，那么可以在最大的表通过mapper 的时候将小表完全
-- 放到内存中。Hive 可以在map 端执行连接过程,这是因为Hive
-- 可以和内存中的小表进行逐一匹配，从而省略掉常规连接操作所需要的reduce 过程。
-- Hive 对于右外连接(RIGHT OUTER JOIN) 和全外连接(FULL OUTER JOIN) 不支持
-- 这个优化。
SELECT/*+ MAPJOIN(d) */ s.ymd, s.symbol, s.price_close, d.dividend
FROM stocks s
         JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
WHERE s.symbol = 'AAPL'
LIMIT 10;

set hive.auto.convert.join=true;

SELECT s.ymd, s.symbol, s.price_close
FROM stocks s
ORDER BY s.ymd ASC, s.symbol DESC
LIMIT 10;

SELECT s.ymd, s.symbol, s.price_close
FROM stocks s
    SORT BY
     s.ymd ASC,
     s.symbol DESC
LIMIT 50;

SELECT s.ymd, s.symbol, s.price_close
FROM stocks s
    DISTRIBUTE BY
     s.symbol
    SORT BY
     s.symbol ASC,
     s.ymd ASC
LIMIT 50;

-- 使用DISTRIBUTE BY …SORT BY 语句或其简化版的CLUSTER BY 语句会剥夺SORT
-- BY 的并行性，然而这样可以实现输出文件的数据是全局排序的。
SELECT s.ymd, s.symbol, s.price_close
FROM stocks s
    CLUSTER BY s.symbol
LIMIT 100;

SELECT (2.0 * cast(cast(b AS STRING) AS DOUBLE))
FROM src;

-- 分桶语句中的分母表示的是数据将会被散列的桶的个数，而分子表示将会选择的桶的个数：
CREATE TABLE numbers (number INT);
LOAD DATA INPATH "/data/number_table_data" INTO TABLE numbers;
SELECT *
FROM numbers TABLESAMPLE (BUCKET 3 OUT OF 10 ON rand()) s;

SELECT*
FROM numbers TABLESAMPLE (BUCKET 3 OUT OF 10 ON rand()) s;

-- 基于行数数据块抽样，这种抽样方式不一定适用于所有的文件格式。另外，这种抽样的最小抽
-- 样单元是一个HDFS 数据块。因此，如果表的数据大小小于普通的块大
-- 小128MB 的话，那么将会返回所有行。
SELECT* FROM numbersflat TABLESAMPLE(0.1 PERCENT) s;

CREATE TABLE numbers_bucketed
(
    number int
) CLUSTERED BY (number) INTO
    1 BUCKETS;
SET hive.enforce.bucketing=true;
INSERT OVERWRITE TABLE numbers_bucketed SELECT number FROM numbers;


-- UNION ALL
SELECT log.ymd, log.level, log.message
FROM (
         SELECT l1.ymd,
                l1.level,
                l1.message,
                'Logl' AS source
         FROM logl l1
         UNION ALL
         SELECT l2.ymd,
                l2.level,
                l2.message,
                'Log2' AS source
         FROM logl l2
     ) log
    SORT BY log.ymd ASC;

FROM (
         FROM src
         SELECT src.key, src.value
         WHERE src.key < 100
         UNION ALL
         FROM src
         SELECT src.key, src.value
         WHERE src.key > 110
     ) unioninput
INSERT
OVERWRITE
DIRECTORY
'/tmp/union.out'
SELECT unioninput.*