-- 如果使用了LOCAL 这个关键字，那么这个路径应该为本地文件系统路径。数据将会被
-- 拷贝到目标位置。如果省略掉LOCAL 关键字，那么这个路径应该是分布式文件系统中
-- 的路径。这种情况下，数据是从这个路径转移到目标位置的。
-- 指定了OVERWRITE 关键字，那么目标文件夹中之前存在的数据将会被先删除掉。
-- 于INPATH 子句中使用的文件路径还有一个限制，那就是这个路径下不可以包含任何文件夹。
-- Hive 会验证文件格式是否和表结构定义的一致。例如，如果表在创建时定义的存储格式是
-- SEQUENCEFILE,那么转载进去的文件也应该是sequencefile 格式的才行。
LOAD DATA LOCAL INPATH '${env:HOME}/california-employees'
    OVERWRITE INTO TABLE employees
    PARTITION (country = 'US', state = 'CA');

-- 使用了OVERWRITE 关键字，因此之前分区中的内容（如果是非分区表，就是之
-- 前表中的内容）将会被覆盖掉。
-- 如果没有使用OVERWRITE 关键字或者使用INTO 关键字替换掉它的话，那么Hive
-- 将会以追加的方式写入数据而不会覆盖掉之前已经存在的内容
INSERT overwrite TABLE employees
    PARTITION (country = 'US', state = 'OR')
SELECT *
FROM staged_employees se
WHERE se.cnty = 'US'
  AND se.st = 'OR';

FROM staged_employees se
INSERT OVERWRITE TABLE employees
    PARTITION (country='US', state='OR')
    SELECT * WHERE se.cnty ='US'AND se.st ='OR'
INSERT OVERWRITE TABLE employees
    PARTITION (country='US', state ='CA')
    SELECT * WHERE se.cnty ='US'AND se.st='CA'
INSERT OVERWRITE TABLE employees
    PARTITION (country='US', state ='IL')
    SELECT * WHERE se.cnty ='US'AND se.st='IL';

-- 动态分区插入
INSERT OVERWRITE TABLE employees
    PARTITION (country, state)
SELECT other_colums, se.cnty, se.st
FROM staged_employees se;

-- 混合使用动态和静态分区,静态分区键必须出现在动态分区键之前。
INSERT OVERWRITE TABLE employees
    PARTITION (country = 'US', state)
SELECT other_colums, se.cnty, se.st
FROM staged_employees se
WHERE se.cnty = 'US';

-- 单个查询语句中创建表并加载数据
CREATE TABLE ca_employees
AS SELECT name, salary, address
   FROM employees
   WHERE se.state='CA';

-- 导出数据
hadoop fs -cp source_path target_path

INSERT OVERWRITE LOCAL DIRECTORY'/tmp/ca_employees'
SELECT name, salary, address
FROM employees
WHERE se. state ='CA';

-- 指定多个输出文件夹目录的
FROM staged_employees se
INSERT OVERWRITE DIRECTORY'/tmp/or_employees'
    SELECT* WHERE se.cty ='US'and se.st='OR'
INSERT OVERWRITE DIRECTORY'/tmp/ca_employees'
    SELECT* WHERE se.cty ='US'and se.st ='CA'
INSERT OVERWRITE DIRECTORY'/tmp/il_employees'
    SELECT* WHERE se.cty ='US'and se.st ='IL';










