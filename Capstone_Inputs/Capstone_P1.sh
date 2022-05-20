find . -name "*.avsc" -exec rm {} \;
find . -name "*.java" -exec rm {} \;
rm -r Capstone_Outputs
mkdir Capstone_Outputs
cp -r /home/anabig114212/Capstone_Inputs/* /home/anabig114212/
mysql -u anabig114212 -pBigdata123 -D anabig114212 -e 'source CreateMySQLTables.sql' > /home/anabig114212/Capstone_Outputs/Cap_MySQLTables.txt
hdfs dfs -rm -r /user/anabig114212/hive/warehouse/Capstone
hdfs dfs -mkdir /user/anabig114212/hive/warehouse/Capstone
sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114212 --username anabig114212 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114212/hive/warehouse/Capstone --m 1 --driver com.mysql.jdbc.Driver
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
hive -f HiveDB.hql > /home/anabig114212/Capstone_Outputs/Cap_HiveDB.txt 
impala-shell -i ip-10-1-2-103.ap-south-1.compute.internal -f EDA.sql > /home/anabig114212/Capstone_Outputs/Cap_ImpalaAnalysis.txt
hive -f HiveTables.sql > /home/anabig114212/Capstone_Outputs/Cap_HiveTables.txt
spark-submit capstone.py > /home/anabig114212/Capstone_Outputs/Cap_SparkSQL_EDA_ML.txt
hdfs dfs -copyToLocal /user/anabig114212/random_forest.model /home/anabig114212/Capstone_Outputs/
zip -r /home/anabig114212/Capstone_Outputs/random_forest.model.zip /home/anabig114212/Capstone_Outputs/random_forest.model 
rm -r /home/anabig114212/Capstone_Outputs/random_forest.model
hdfs dfs -copyToLocal /user/anabig114212/logistic_regression.model /home/anabig114212/Capstone_Outputs/
zip -r /home/anabig114212/Capstone_Outputs/logistic_regression.model.zip /home/anabig114212/Capstone_Outputs/logistic_regression.model 
rm -r /home/anabig114212/Capstone_Outputs/logistic_regression.model 