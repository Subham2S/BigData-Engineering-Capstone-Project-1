USE anabig114212;

START TRANSACTION;

DROP TABLE IF EXISTS salaries;
DROP TABLE IF EXISTS dept_manager;
DROP TABLE IF EXISTS dept_emp;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS titles;

CREATE TABLE departments (
    dept_no VARCHAR(10)  NOT NULL ,
    dept_name VARCHAR(30)  NOT NULL ,
    PRIMARY KEY (
        dept_no
    )
);

CREATE TABLE titles (
    title_id VARCHAR(10)  NOT NULL ,
    title VARCHAR(30)  NOT NULL ,
    PRIMARY KEY (
        title_id
    )
);

CREATE TABLE employees (
    emp_no INT  NOT NULL ,
    emp_title_id VARCHAR(10)  NOT NULL ,
    birth_date VARCHAR(10) NOT NULL ,
    first_name VARCHAR(30)  NOT NULL ,
    last_name VARCHAR(30)  NULL ,
    sex CHAR(1)  NOT NULL ,
    hire_date VARCHAR(10)  NOT NULL ,
    no_of_projects INT  NOT NULL ,
    last_performance_rating VARCHAR(10)  NOT NULL ,
    left2 TINYINT  NOT NULL ,
    last_date VARCHAR(10) NULL ,
    PRIMARY KEY (
        emp_no
    )
);
ALTER TABLE employees ADD CONSTRAINT fk_employees_emp_title_id FOREIGN KEY(emp_title_id)
REFERENCES titles (title_id);


CREATE TABLE dept_emp (
    emp_no INT  NOT NULL ,
    dept_no VARCHAR(10)  NOT NULL 
);
ALTER TABLE dept_emp ADD CONSTRAINT fk_dept_emp_emp_no FOREIGN KEY(emp_no)
REFERENCES employees (emp_no);

ALTER TABLE dept_emp ADD CONSTRAINT fk_dept_emp_dept_no FOREIGN KEY(dept_no)
REFERENCES departments (dept_no);


CREATE TABLE dept_manager (
    dept_no VARCHAR(10)  NOT NULL ,
    emp_no INT  NOT NULL    
);
ALTER TABLE dept_manager ADD CONSTRAINT fk_dept_manager_emp_no FOREIGN KEY(emp_no)
REFERENCES employees (emp_no);

ALTER TABLE dept_manager ADD CONSTRAINT fk_dept_manager_dept_no FOREIGN KEY(dept_no)
REFERENCES departments (dept_no);

CREATE TABLE salaries (
    emp_no INT  NOT NULL ,
    salary INT  NOT NULL 
);
ALTER TABLE salaries ADD CONSTRAINT fk_salaries_emp_no FOREIGN KEY(emp_no)
REFERENCES employees (emp_no);

LOAD DATA LOCAL INFILE '/home/anabig114212/departments.csv' INTO TABLE departments FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/home/anabig114212/titles.csv' INTO TABLE titles FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/home/anabig114212/employees.csv' INTO TABLE employees FIELDS TERMINATED BY ','OPTIONALLY ENCLOSED BY ''  LINES TERMINATED BY '\n' IGNORE 1 ROWS 
SET birth_date = STR_TO_DATE(birth_date, "%m/%d/%Y"), hire_date = STR_TO_DATE(hire_date, "%m/%d/%Y"), last_date = STR_TO_DATE(last_date, "%m/%d/%Y");
LOAD DATA LOCAL INFILE '/home/anabig114212/dept_manager.csv' INTO TABLE dept_manager FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/home/anabig114212/dept_emp.csv' INTO TABLE dept_emp FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/home/anabig114212/salaries.csv' INTO TABLE salaries FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

COMMIT;

SELECT * FROM departments LIMIT 10;
SELECT * FROM titles LIMIT 10;
SELECT * FROM employees LIMIT 10;
SELECT * FROM dept_manager LIMIT 10;
SELECT * FROM dept_emp LIMIT 10;
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
SELECT 'salaries', count(*) FROM salaries;