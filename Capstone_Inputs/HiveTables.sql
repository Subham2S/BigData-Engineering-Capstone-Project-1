USE anabig114212_cap;

SET mapreduce.input.fileinputformat.split.maxsize = 67108864;
SET mapreduce.map.memory.mb = 2048;
SET mapreduce.reduce.memory.mb = 2048;
SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

DROP TABLE IF EXISTS dept_emp1;
CREATE TABLE dept_emp1 AS
SELECT a.emp_no, a.dept_no 
FROM(SELECT *, row_number() over(PARTITION BY emp_no ORDER BY dept_no DESC) rnk FROM dept_emp)a
WHERE a.rnk = 1;

SELECT * FROM departments LIMIT 10;
SELECT * FROM titles LIMIT 10;
SELECT * FROM employees LIMIT 10;
SELECT * FROM dept_manager LIMIT 10;
SELECT * FROM dept_emp LIMIT 10;
SELECT * FROM dept_emp1 LIMIT 10;
SELECT * FROM salaries LIMIT 10;

SELECT 'departments' AS Table_Name, count(*) AS Records FROM departments
UNION
SELECT 'titles', count(*) FROM titles
UNION
SELECT 'employees', count(*) FROM employees
UNION
SELECT 'dept_manager', count(*) FROM dept_manager
UNION
SELECT 'dept_emp', count(*) FROM dept_emp
UNION
SELECT 'dept_emp1', count(*) FROM dept_emp1
UNION
SELECT 'salaries', count(*) FROM salaries;