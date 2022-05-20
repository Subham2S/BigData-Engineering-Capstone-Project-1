-- Database
USE anabig114212_cap;
SHOW TABLES;
-- Table Stats for Employees Table
COMPUTE STATS employees;
SHOW COLUMN STATS employees;

-- 1. A list showing employee number, last name, first name, sex, and salary for each employee
SELECT e.emp_no, last_name, first_name, sex, salary 
FROM employees e
JOIN salaries s  ON e.emp_no = s.emp_no
LIMIT 30;

-- 2. A list showing first name, last name, and hire date for employees who were hired in 1986.
SELECT first_name, last_name, hire_date
FROM employees
WHERE year(hire_date) = 1986
LIMIT 30;

-- 3. A list showing the manager of each department with the following information: department number, department name, the manager's employee number, last name, first name.
SELECT dm.dept_no, d.dept_name, e.emp_no, e.last_name, e.first_name, t.title
FROM dept_manager dm 
JOIN departments d ON dm.dept_no = d.dept_no
JOIN employees e ON dm.emp_no = e.emp_no
JOIN titles t ON e.emp_title_id = t.title_id AND t.title = 'Manager';

-- 4. A list showing the department of each employee with the following information: employee number, last name, first name, and department name. 
WITH dept_emp3 AS
(SELECT a.emp_no, a.dept_no 
FROM(SELECT *, row_number() over(PARTITION BY emp_no ORDER BY dept_no DESC) rnk FROM dept_emp)a
WHERE a.rnk = 1)
SELECT e.emp_no, last_name, first_name, d.dept_name 
FROM employees e 
JOIN dept_emp3 de ON e.emp_no = de.emp_no 
JOIN departments d ON de.dept_no = d.dept_no
LIMIT 30;

-- 5. A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B".
SELECT first_name, last_name, sex 
FROM employees 
WHERE first_name = 'Hercules' AND last_name LIKE 'B%'
LIMIT 100;

-- 6. A list showing all employees in the Sales department, including their employee number, last name, first name, and department name.
WITH dept_emp3 AS
(SELECT a.emp_no, a.dept_no 
FROM(SELECT *, row_number() over(PARTITION BY emp_no ORDER BY dept_no DESC) rnk FROM dept_emp)a
WHERE a.rnk = 1)
SELECT e.emp_no, e.last_name, e.first_name, d.dept_name
FROM employees e 
JOIN dept_emp3 de ON e.emp_no = de.emp_no 
JOIN departments d ON de.dept_no = d.dept_no AND d.dept_name = 'Sales'
LIMIT 100;

-- 7. A list showing all employees in the Sales and Development departments, including their employee number, last name, first name, and department name.
WITH dept_emp3 AS
(SELECT a.emp_no, a.dept_no 
FROM(SELECT *, row_number() over(PARTITION BY emp_no ORDER BY dept_no DESC) rnk FROM dept_emp)a
WHERE a.rnk = 1)
SELECT e.emp_no, e.last_name, e.first_name, d.dept_name
FROM employees e 
JOIN dept_emp3 de ON e.emp_no = de.emp_no 
JOIN departments d ON de.dept_no = d.dept_no AND d.dept_name IN ('Sales','development')
LIMIT 100;

-- 8. A list showing the frequency count of employee last names, in descending order. (i.e., how many employees share each last name)
SELECT last_name, count(*) AS Last_name_cnt
FROM employees
GROUP BY last_name
ORDER BY Last_name_cnt DESC
LIMIT 100;

-- 11. Calculate employee tenure & show the tenure distribution among the employees
WITH a AS
(SELECT greatest(max(last_date), max(hire_date)) maxDate 
FROM employees),
t AS
(SELECT 
    CASE WHEN last_date IS NULL THEN datediff(a.maxDate, hire_date)
    ELSE datediff(last_date, hire_date) END AS Duration, 
    count(*) AS Duration_cnt
FROM employees,a
GROUP BY Duration)
SELECT Duration_cnt, count(Duration) Duration_Dist FROM t
GROUP BY Duration_cnt
ORDER BY Duration_Dist DESC
LIMIT 100;

-- 12. Count of Employee Status (Currently working or Left) in different departments grouped by gender
WITH dept_emp3 AS
(SELECT a.emp_no, a.dept_no 
FROM(SELECT *, row_number() over(PARTITION BY emp_no ORDER BY dept_no DESC) rnk FROM dept_emp)a
WHERE a.rnk = 1)
SELECT
    dept_name, sex,
    count(Left2) Total_Count,
    sum(CASE WHEN Left2 = 0 THEN 1 ELSE 0 END) Working_Count,
    sum(Left2) Left_Count
FROM Employees e
JOIN dept_emp3 de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
GROUP BY dept_name, sex;

-- 13 Max, Min and Avg age of Employees in diffrent departments
WITH a AS
(SELECT greatest(max(last_date), max(hire_date)) maxDate 
FROM employees),
dept_emp3 AS
(SELECT a.emp_no, a.dept_no 
FROM(SELECT *, row_number() over(PARTITION BY emp_no ORDER BY dept_no DESC) rnk FROM dept_emp)a
WHERE a.rnk = 1)
SELECT dept_name, min(Age), max(Age), avg(Age)
FROM (
        SELECT
            dept_name, e.emp_no, year(maxDate)-year(birth_date) Age
        FROM Employees e, a
        JOIN dept_emp3 de ON e.emp_no = de.emp_no
        JOIN departments d ON de.dept_no = d.dept_no 
        )a1
GROUP BY dept_name;

-- 14 Count of Employees in various titles
SELECT 
    t.title, 
    count(Salary) Emp_Count
FROM salaries s
JOIN employees e ON s.emp_no = e.emp_no
JOIN titles t ON e.emp_title_id = t.title_id
GROUP BY t.title
ORDER BY Emp_Count DESC;

-- 15 Average Tenure Distribution accross Departments
WITH a AS
(SELECT greatest(max(last_date), max(hire_date)) maxDate 
FROM employees)
SELECT 
    avg(CASE WHEN last_date IS NULL 
            THEN datediff(maxDate,hire_date)
    ELSE datediff(last_date,hire_date) END) AS Avg_Tenure_Years, 
    dept_name
FROM employees e,a
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
GROUP BY dept_name
ORDER BY Avg_Tenure_Years DESC

--16 Average Tenure Distribution accross Titles
WITH a AS
(SELECT greatest(max(last_date), max(hire_date)) maxDate 
FROM employees)
SELECT 
    avg(CASE WHEN last_date IS NULL 
            THEN datediff(maxDate,hire_date)
    ELSE datediff(last_date,hire_date) END) AS Avg_Tenure_Years, 
    title
FROM employees_at e
JOIN titles t ON e.emp_title_id = t.title_id
GROUP BY title
ORDER BY Avg_Tenure_Years DESC;


exit;
