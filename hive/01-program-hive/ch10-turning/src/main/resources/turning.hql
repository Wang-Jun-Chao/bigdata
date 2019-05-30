CREATE TABLE onecol(number INT);
LOAD DATA INPATH "/data/onecol_table_data" INTO TABLE onecol;

SELECT* FROM onecol;
SELECT SUM(number) FROM onecol;

EXPLAIN SELECT SUM(number) FROM onecol;

EXPLAIN EXTENDED SELECT SUM(number) FROM onecol;

-- 严格模式
-- 通过设置属性hive.mapred.mode 值为strict 可以禁止3 种类型的查询。
-- 其一，对千分区表，除非W田rnRE 语句中含有分区字段过滤条件来限制数据范围，否
-- 则不允许执行。换句话说，就是用户不允许扫描所有分区。
-- 其二，对于使用了ORDER BY 语句的查询，要求必须使用LIMIT 语句。
-- 其三，也就是最后一种情况，就是限制笛卡尔积的查询。

-- 调整mapper 和reducer 个数

-- JVM 重用
-- Hadoop 的默认配置通常是使用派生NM 来执行map 和reduce 任务的。这时NM 的启
-- 动过程可能会造成相当大的开销，尤其是执行的job 包含有成百上千个task 任务的情况。
-- NM 重用可以使得NM 实例在同一个job 中重新使用N 次。N 的值可以在Hadoop 的
-- mapred-site.xml 文件,
--     <property>
--         <name>mapred.job.reuse.jvm.num.tasks</name>
--         <value>10</value>
--         <description>How many tasks to run per jvm. If set to -1, there is no limit.</description>
--     </property>
-- 这个功能的一个缺点是，开启NM 重用将会一直占用使用到的task 插槽，以便进行重
-- 用，直到任务完成后才能释放

-- 动态分区调整

-- 推测执行

-- 单个Map Reduce 中多个GROUP BY

-- 虚拟列

