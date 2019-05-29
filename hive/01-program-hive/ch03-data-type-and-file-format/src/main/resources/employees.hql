CREATE TABLE employees
(
    name         STRING,
    salary       FLOAT,
    subordinates ARRAY<STRING>,
    deductions   MAP<STRING, FLOAT>,
    address      STRUCT<street: STRING, city: STRING, state: STRING, zip: INT>
);