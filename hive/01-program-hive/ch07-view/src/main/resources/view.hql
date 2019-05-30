DROP TABLE IF EXISTS userinfo;
CREATE TABLE userinfo
(
    firstname STRING,
    lastname  STRING,
    ssn       STRING,
    password  STRING
);

CREATE VIEW safer_user_info AS
SELECT firsTname, lastname
FROM userinfo;

CREATE TABLE employee
(
    firstname  STRING,
    lastname   STRING,
    ssn        STRING,
    password   STRING,
    department STRING
);
CREATE VIEW techops_employee AS
SELECT firstname, lastname, ssn
FROM userinfo
WHERE department = 'techops';

CREATE EXTERNAL TABLE dynamictable
(
    cols map<string,string>
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        COLLECTION ITEMS TERMINATED BY ';'
        MAP KEYS TERMINATED BY ':'
    STORED AS TEXTFILE;

LOAD DATA INPATH "/data/dynamictable_table_data" INTO TABLE dynamictable;

CREATE VIEW orders(state, city, part) AS
SELECT cols["state"], cols["city"], cols["part"]
FROM dynamictable
WHERE cols["type"] = "request";

CREATE VIEW shipments(`time`, part) AS
SELECT cols["time"], cols["part"]
FROM dynamictable
WHERE cols["type"] = "response";

CREATE OR REPLACE VIEW shipments(`time`, part)
    COMMENT 'Time and parts for shipments.'
    TBLPROPERTIES ('creator' = 'me')
AS
SELECT cols["time"], cols["part"]
FROM dynamictable
WHERE cols["type"] = "response";

-- 复制表
CREATE TABLE shipments2
    LIKE shipments;

-- 删除视图
DROP VIEW IF EXISTS shipments;
-- 视图只允许改变元数据中TBLPROPERTIES 属性信息：
ALTER VIEW shipments SET TBLPROPERTIES ('created_at'='some_ time stamp') ;