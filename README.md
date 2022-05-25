 
# **BigData Engineering Capstone Project 1**
## Summary
Objective is to work on data engineering project for one of the big corporation's employee's data from the 1980s and 1995s. All the database of employees from that period are provided six CSV files. In this project, I have designed data model with all the tables to hold data, imported the CSVs into a SQL database, transferred SQL database to HDFS/Hive, and performed analysis using Hive/Impala/Spark/SparkML using the data and created data and ML pipelines.

## Process
Importing data from MySQL RDBMS to HDFS using Sqoop, Creating HIVE Tables with compressed file format (avro), Explanatory Data Analysis with Impala & SparkSQL and Building Random Forest Classifer Model & Logistic Regression Model using SparkML.

### Step-1
**Upload the Capstone_Inputs Folder in Client home dir which contains :**
- Capstone_P1.sh
- CreateMySQLTables.sql
- EDA.sql
- HiveTables.sql
- HiveDB.hql
- capstone.py 
- departments.csv
- dept_emp.csv
- dept_manager.csv
- employees.csv
- salaries.csv
- titles.csv

### Step-2
**Run the Capstone_P1.sh file in Terminal**
```console
$ sh /home/anabig114212/Capstone_Inputs/Capstone_P1.sh
```
### Step-3
**Wait for a while and download the Capstone_Outputs Folder** <br>
After approx. 10-15 mins Capstone_Ouputs Folder will be generated with all the output files : <br>
 **1. Cap_MySQLTables.txt** -  To Check MySQL Tables. <br>
 **2. Cap_HiveDB.txt** - To Ensure that Hive Tables were created. <br>
 **3. Cap_ImpalaAnalysis.txt** - All EDA output tables from Impala. <br>
 **4. Cap_HiveTables.txt** - To Check records in Hive Tables and dept_emp1 is created additionally to fix some duplicate issues which were present in dept_emp. <br>
 **5. Cap_SparkSQL_EDA_ML.txt** - All EDA output tables from SparkSQL, pySpark and all the details of the Models (both Random Forest & Logistic Regression) <br>
 **6. random_forest.model.zip** <br>
 **7. logistic_regression.model.zip** <br>

## Details of Capstone_P1.sh

### Linux Commands
```bash
find . -name "*.avsc" -exec rm {} \;
```
- Removes the metadata of the tables which are there in the Root dir (created by the sqoop command when the code was run last time)
```bash
find . -name "*.java" -exec rm {} \;
```
- Removes the Java MapReduce Codes which are there in the Root dir (created by the sqoop command when the code was run last time)
```bash
rm -r /home/anabig114212/Capstone_Outputs
mkdir /home/anabig114212/Capstone_Outputs
```
- Removes the current Capstone_Outputs Folder and Creates a new dir "Capstone_Outputs" - Here all the Outputs will be stored.
```bash
cp -r /home/anabig114212/Capstone_Inputs/* /home/anabig114212/
```
- Recursively Copies everything to root folder to avoid permission issues at later point of time.

### MySQL (.sql)
```bash
mysql -u anabig114212 -pBigdata123 -D anabig114212 -e 'source CreateMySQLTables.sql' > /home/anabig114212/Capstone_Outputs/Cap_MySQLTables.txt
```
- Creates MySQL tables & Inserts data
```bash
hdfs dfs -rm -r /user/anabig114212/hive/warehouse/Capstone
hdfs dfs -mkdir /user/anabig114212/hive/warehouse/Capstone
```
- Removes & Creates the Warehouse/Capstone dir to avoid anomalies between same named files

### sqoop
```bash
sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114212 --username anabig114212 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114212/hive/warehouse/Capstone --m 1 --driver com.mysql.jdbc.Driver
```
- Importing Data & Metadata of all the Tables from MySQL RDBMS system to Hadoop using SQOOP command

### HDFS Commands
```bash
hdfs dfs -rm -r /user/anabig114212/hive/avsc
hdfs dfs -mkdir /user/anabig114212/hive/avsc
hdfs dfs -put  departments.avsc /user/anabig114212/hive/avsc/departments.avsc
hdfs dfs -put  titles.avsc /user/anabig114212/hive/avsc/titles.avsc
hdfs dfs -put  employees.avsc /user/anabig114212/hive/avsc/employees.avsc
hdfs dfs -put  dept_manager.avsc /user/anabig114212/hive/avsc/dept_manager.avsc
hdfs dfs -put  dept_emp.avsc /user/anabig114212/hive/avsc/dept_emp.avsc
hdfs dfs -put  salaries.avsc /user/anabig114212/hive/avsc/salaries.avsc
hadoop fs -chmod +rwx /user/anabig114212/hive/avsc/*
hadoop fs -chmod +rwx /user/anabig114212/hive/warehouse/Capstone/*
```
- Transfering the Metadata to HDFS for Table creation in Hive

### Hive (.hql)
```bash
hive -f HiveDB.hql > /home/anabig114212/Capstone_Outputs/Cap_HiveDB.txt 
```
- Basically all the hive Tables are created as AVRO format. In the .hql file Table location and its metadata (schema) locations are mentioned seperately.

### Impala (.sql)
```bash
impala-shell -i ip-10-1-2-103.ap-south-1.compute.internal -f EDA.sql > /home/anabig114212/Capstone_Outputs/Cap_ImpalaAnalysis.txt
```
- Explanatory Data Analysis is done with Impala. 
```bash
hive -f HiveTables.sql > /home/anabig114212/Capstone_Outputs/Cap_HiveTables.txt
```
- Checking all the records of the Hive Tables before moving to spark.

### Spark (.py)
```bash
spark-submit capstone.py > /home/anabig114212/Capstone_Outputs/Cap_SparkSQL_EDA_ML.txt
```
- This capstone.py does everything. First it loads the tables and creates spark dataframes, then checks all the records again. After that Same EDA analysis is performed with the aid of sparkSQL & pySpark. 
- After EDA, it checks stats for Numerical & Categorical Variables. Then proceeds towards model building after creating final df with joining the tables and dropping irrelevant columns. As per the chosen target variable 'left', the independent variables were divided into continuous and categorical variables, and in the categorical variables, two columns were label encoded manually and the rest were processed for One-Hot Encoding. 
- Then, based on previous experience of EDA, both Random Forest Classification Model and Logistic Regression Model are chosen for this dataset. And as per the analysis the accuracies were 99% (RF) and 90% (LR). Model were fitted on test and train (0.3: 0.7) and gave same accuracy. Considering these as good fits, both the models were saved.
- After that a Pipeline was created and same analysis were performed in a streamlined manner to build these models. The Accuracies between the built models and the Pipeline models are very close. The reason behind the slight change in the accuracies is that the earlier case, the train & test split was performed after fitting the assembler but in case of ML pipeline, the assembler is inside the stages, so assembler is fitting on split datasets separately as a part of the pipeline. This is also clearly visible in the features column as well. So, this was a good test of the pipeline models in terms of accuracy, and we can conclude that the ML Pipeline is working properly.


### Collecting the Models
```bash
hdfs dfs -copyToLocal /user/anabig114212/random_forest.model /home/anabig114212/Capstone_Outputs/
zip -r /home/anabig114212/Capstone_Outputs/random_forest.model.zip /home/anabig114212/Capstone_Outputs/random_forest.model 
rm -r /home/anabig114212/Capstone_Outputs/random_forest.model
hdfs dfs -copyToLocal /user/anabig114212/logistic_regression.model /home/anabig114212/Capstone_Outputs/
zip -r /home/anabig114212/Capstone_Outputs/logistic_regression.model.zip /home/anabig114212/Capstone_Outputs/logistic_regression.model 
rm -r /home/anabig114212/Capstone_Outputs/logistic_regression.model
```

## Reference Files
The following files are added for your reference.
1. Capstone.ipynb
2. Capstone Project1.pptx, Capstone Project1.pdf 
2. Capstone.zip
3. ERD_Data Model.jpg, ERD_Data Model.svg

 
