-- Hive 可以通过$HIVE_HOME/conf 目录下的2 个Log4J 配置文件来配置日志。其中
-- hive-log4j.properties 文件用来控制CLI 和其他本地执行组件的日志，而
-- hive-exec-log4j.properties 文件用来控制MapReduce task 内的日志，这个文件并非必须
-- 在Hive 安装目录下存在，因为Hive JAR 包中已经包含了默认属性值。事实上， conf
-- 目录下的文件是带有template 文件扩展名的，因此默认是不会被加载的。如果想使用
-- 这2 个配置，只需要将template 扩展名去掉

-- 也可以临时地改变日志配置而无需拷贝和修改Log4J 文件。在Hive Shell 启动时可以通
-- 过hiveconf 参数指定log4.properties 文件中的任意属性
$ bin/hive -hiveconf hive.root.logger=DEBUG,console

-- 连接Java 调试器到Hive
bin/hive --help --debug

CREATE TABLE dest1
(
    cl INT,
    c2 DOUBLE,
    c3 DOUBLE,
    c4 DOUBLE,
    cs INT,
    c6 STRING,
    c7 INT
);
CREATE TABLE src(key INT);

EXPLAIN
    FROM src
    INSERT
    OVERWRITE
    TABLE
    dest1
SELECT 3 + 2,
       3.0 + 2,
       3 + 2.0,
       3.0 + 2.0,
       3 + CAST(2.0 AS INT) + CAST(CAST(0 AS SMALLINT) AS INT),
       CAST(1 AS BOOLEAN),
       CAST(TRUE AS INT)
WHERE src.key = 86;

FROM src
INSERT
OVERWRITE
TABLE
dest1
SELECT 3 + 2,
       3.0 + 2,
       3 + 2.0,
       3.0 + 2.0,
       3 + CAST(2.0 AS INT) + CAST(CAST(0 AS SMALLINT) AS INT),
       CAST(1 AS BOOLEAN),
       CAST(TRUE AS INT)
WHERE src.key = 86;









