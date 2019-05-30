DROP TABLE employees;
CREATE TABLE employees
(
    name         STRING,
    salary       FLOAT,
    subordinates ARRAY<STRING>,
    deductions   MAP<STRING, FLOAT>,
    address      STRUCT<street: STRING, city:STRING, state:STRING, zip:INT>
)
    PARTITIONED BY (country STRING, state STRING);

-- 创建索引
CREATE INDEX employees_index
    ON TABLE employees (country)
    AS 'org.apache.hadoop.hive.ql.index.compact.CompactindexHandler'
    WITH DEFERRED REBUILD
    IDXPROPERTIES ('creator' = 'me','created_at' = 'some_time')
    IN TABLE employees_index_table
COMMENT'Employees indexed by country and name.';

-- bitmap 索引普遍应用于排重后值较少的列。
CREATE INDEX employees_index
    ON TABLE employees (country)
    AS 'BITMAP'
    WITH DEFERRED REBUILD
    IDXPROPERTIES ('creator' = 'me','created at' = 'some time' )
    IN TABLE employees_index_table
    COMMENT 'Employees indexed by country and name.';

-- 重建索引.如果重建索引失败，那么在重建开始之前，索引将停留在之前的版本状态。从这种意
-- 义上看，重建索引操作是原子性的。
ALTER INDEX employees_index
    ON employees
    PARTITION (country = 'US')
    REBUILD;
-- 显示索引
SHOW FORMATTED INDEX ON employees;
-- 删除索引
DROP INDEX IF EXISTS employees_index ON employees;

-- 实现一个定制化的索引处理器
-- 用户也是可以使用org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler 中的源代码作为例子来学习的

FROM history
    INSERT OVERWRITE sales   SELECT * WHERE action='purchased'
    INSERT OVERWRITE credits SELECT*  WHERE action='returned';