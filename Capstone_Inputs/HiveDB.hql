DROP DATABASE IF EXISTS anabig114212_cap CASCADE;
CREATE DATABASE anabig114212_cap;
USE anabig114212_cap;

CREATE EXTERNAL TABLE departments STORED AS AVRO LOCATION '/user/anabig114212/hive/warehouse/Capstone/departments' TBLPROPERTIES ('avro.schema.url'='/user/anabig114212/hive/avsc/departments.avsc');
CREATE EXTERNAL TABLE titles STORED AS AVRO LOCATION '/user/anabig114212/hive/warehouse/Capstone/titles' TBLPROPERTIES ('avro.schema.url'='/user/anabig114212/hive/avsc/titles.avsc');
CREATE EXTERNAL TABLE employees STORED AS AVRO LOCATION '/user/anabig114212/hive/warehouse/Capstone/employees' TBLPROPERTIES ('avro.schema.url'='/user/anabig114212/hive/avsc/employees.avsc');
CREATE EXTERNAL TABLE dept_manager STORED AS AVRO LOCATION '/user/anabig114212/hive/warehouse/Capstone/dept_manager' TBLPROPERTIES ('avro.schema.url'='/user/anabig114212/hive/avsc/dept_manager.avsc');
CREATE EXTERNAL TABLE dept_emp STORED AS AVRO LOCATION '/user/anabig114212/hive/warehouse/Capstone/dept_emp' TBLPROPERTIES ('avro.schema.url'='/user/anabig114212/hive/avsc/dept_emp.avsc');
CREATE EXTERNAL TABLE salaries STORED AS AVRO LOCATION '/user/anabig114212/hive/warehouse/Capstone/salaries' TBLPROPERTIES ('avro.schema.url'='/user/anabig114212/hive/avsc/salaries.avsc');