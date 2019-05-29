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
SELECT* FROM employees;
SELECT* FROM employees WHERE country='US'AND state='CA'LIMIT 100;
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
WHERE `exchange` ='NASDAQ' AND symbol='AAPL'
GROUP BY year (ymd)
HAVING avg(price_close) > 50.0;

SELECT a.ymd, a.price_close, b.price_close
FROM stocks a
         JOIN stocks b ON a.ymd = b.ymd
WHERE a.symbol = 'AAPL'
  AND b.symbol = 'IBM' LIMIT 10;

-- Hive 中支持的查询语句
SELECT a.ymd, a.price_close, b.price_close
FROM stocks a
         JOIN stocks b
              ON a.ymd <= b.ymd
WHERE a.symbol = 'AAPL'
  AND b.symbol = 'IBM' LIMIT 10;

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
WHERE s.symbol = 'AAPL' LIMIT 10;
















