'''
    BigData Engineering Capstone Project 1
'''

# # **Loading Hive Tables and Data Preparation for Analysis**

# In[125]:

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions \
    import col, year, to_date, greatest, count, max
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.feature \
    import OneHotEncoderEstimator, StringIndexer, VectorAssembler

spark = SparkSession.builder.appName("Subham_Capstone").config(
    "hive.metastore.uris",
    "thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083").config(
        "spark.sql.catalogImplementation=hive").config(
        "spark.sql.warehouse.dir",
        "hdfs://nameservice1/user/anabig114212/hive/Capstone").config(
            "spark.serializer",
    "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('OFF')

# In[126]:

pd.set_option('display.max_columns', None)

# In[127]:

# Creating Local Views and Spark Dataframes to call these objects from memory.

departments = spark.table('anabig114212_cap.departments')
departments.createOrReplaceTempView('departments')

titles = spark.table('anabig114212_cap.titles')
titles.createOrReplaceTempView('titles')

employees = spark.table('anabig114212_cap.employees')
employees = employees.withColumn('birth_date',
                                 to_date(col('birth_date'),
                                         'yyyy-MM-dd'))\
    .withColumn('hire_date',
                to_date(col('hire_date'),
                        'yyyy-MM-dd'))\
    .withColumn('last_date',
                to_date(col('last_date'),
                        'yyyy-MM-dd'))  # Converting to Proper date formats
employees.createOrReplaceTempView('employees')

dept_emp_raw = spark.table('anabig114212_cap.dept_emp')
dept_emp = spark.table('anabig114212_cap.dept_emp1')
dept_emp.createOrReplaceTempView('dept_emp')

dept_manager = spark.table('anabig114212_cap.dept_manager')
dept_manager.createOrReplaceTempView('dept_manager')

salaries = spark.table('anabig114212_cap.salaries')
salaries.createOrReplaceTempView('salaries')


# ## **Validating Data**

# In[128]:

departments.show()
departments.printSchema()
departments.count()


# In[129]:


titles.show()
titles.printSchema()
titles.count()


# In[130]:


employees.printSchema()
employees.count()


# In[131]:


employees.toPandas().head(20)


# In[132]:


dept_emp_raw.show()
dept_emp_raw.printSchema()
dept_emp_raw.count()


# #### dept_emp table was created by removing duplicate emp_no,
# to keep only the last the department for each employee
# [Created in Hive]

# In[133]:


dept_emp.show()
dept_emp.printSchema()
dept_emp.count()


# In[134]:


dept_manager.show()
dept_manager.printSchema()
dept_manager.count()


# In[135]:

salaries.show()
salaries.printSchema()
salaries.count()


# ### Preparing Employees Table for EDA

# #### Maximum date availiable in the Dataset

# In[137]:


maxDateInDataset = employees.select(
    greatest(max(col('last_date')), max(col('hire_date'))))
maxDateInDataset.show()


# In[138]:


maxDateInDataset = employees.select(
    greatest(max(col('last_date')), max(col('hire_date')))).toPandas().loc[0][0]
employees = employees.withColumn(
    'Age',
    maxDateInDataset.year -
    year(
        employees.birth_date)).withColumn(
            'Tenure_Years',
            F.when(
                col('left2') == 1,
                year(
                    employees.last_date) -
                year(
                    employees.hire_date)).otherwise(
                maxDateInDataset.year -
                year(
                    employees.hire_date)))
employees.createOrReplaceTempView('employees_at')
employees.toPandas().head(20)


# # **Exploratory Data Analysis**

# ## **1. A list showing employee number, last name, first name, sex, and salary for each employee**

# In[139]:


spark.sql("""
            SELECT e.emp_no, last_name, first_name, sex, salary
            FROM employees e
            JOIN salaries s  ON e.emp_no = s.emp_no
         """).show()


# In[140]:


employees.join(
    salaries,
    on='emp_no').select(
        'emp_no',
        'last_name',
        'first_name',
        'sex',
    'salary').show()


# ## **2. A list showing first name, last name, and hire date for employees
# who were hired in 1986.**

# In[141]:


spark.sql("""
            SELECT first_name, last_name, hire_date
            FROM employees
            WHERE year(hire_date) = 1986
         """).show()


# In[142]:


employees[year(col('hire_date')) == 1986].select(
    'first_name', 'last_name', 'hire_date').show()


# ## **3. A list showing the manager of each department with the following information:
# department number, department name, the manager's employee number, last name, first name.**

# In[143]:


spark.sql("""
            SELECT dm.dept_no, d.dept_name, e.emp_no, e.last_name, e.first_name, t.title
            FROM dept_manager dm
            JOIN departments d ON dm.dept_no = d.dept_no
            JOIN employees e ON dm.emp_no = e.emp_no
            JOIN titles t ON e.emp_title_id = t.title_id AND t.title = 'Manager'
         """).show()


# In[144]:


dept_manager.join(
    departments,
    on='dept_no').join(
        employees,
        on='emp_no').join(
            titles,
            employees.emp_title_id == titles.title_id).select(
                'dept_no',
                'dept_name',
                'emp_no',
                'last_name',
                'first_name',
    'title').show()


# ## **4. A list showing the department of each employee with the following information:
# employee number, last name, first name, and department name.**

# In[145]:


spark.sql("""
            SELECT e.emp_no, last_name, first_name, d.dept_name
            FROM employees e
            JOIN dept_emp de ON e.emp_no = de.emp_no
            JOIN departments d ON de.dept_no = d.dept_no
         """).show()


# In[146]:


employees.join(
    dept_emp,
    on='emp_no').join(
        departments,
        on='dept_no').select(
            'emp_no',
            'last_name',
            'first_name',
    'dept_name').show()


# ## **5. A list showing first name, last name, and sex for
# employees whose first name is "Hercules" and last names begin with "B"**

# In[147]:


spark.sql("""
            SELECT first_name, last_name, sex
            FROM employees
            WHERE first_name = 'Hercules' AND last_name LIKE 'B%'
         """).show()


# In[148]:


employees[(col('first_name') == 'Hercules') & (col('last_name').like(
    'B%'))].select('first_name', 'last_name', 'sex').show()


# ## **6. A list showing all employees in the Sales department,
# including their employee number, last name, first name, and department name.**

# In[149]:


spark.sql("""
            SELECT e.emp_no, e.last_name, e.first_name, d.dept_name
            FROM employees e
            JOIN dept_emp de ON e.emp_no = de.emp_no
            JOIN departments d ON de.dept_no = d.dept_no AND d.dept_name = 'Sales'
         """).show()


# In[150]:


employees.join(dept_emp,
               on='emp_no').join(departments[col('dept_name') == 'Sales'],
                                 on='dept_no').select('emp_no',
                                                      'last_name',
                                                      'first_name',
                                                      'dept_name').show()


# ## **7. A list showing all employees in the Sales and Development departments,
# including their employee number, last name, first name, and department name.**

# In[151]:


spark.sql("""
            SELECT e.emp_no, e.last_name, e.first_name, d.dept_name
            FROM employees e
            JOIN dept_emp de ON e.emp_no = de.emp_no
            JOIN departments d ON de.dept_no = d.dept_no AND d.dept_name IN ('Sales','development')
         """).show()


# In[152]:


employees.join(dept_emp,
               on='emp_no').join(departments[col('dept_name').isin('Sales',
                                                                   'development')],
                                 on='dept_no').select('emp_no',
                                                      'last_name',
                                                      'first_name',
                                                      'dept_name').show()


# ## **8. A list showing the frequency count of employee last names, in descending order.
# ( i.e., how many employees share each last name**

# In[153]:


spark.sql("""
            SELECT last_name, count(*) AS Last_name_cnt
            FROM employees
            GROUP BY last_name
            ORDER BY Last_name_cnt DESC
         """).show()


# In[154]:


employees.groupBy('last_name').agg(
    count('emp_no').alias('Last_name_cnt')).orderBy(
        col('Last_name_cnt').desc()).show()



# ## **10. Bar graph to show the Average salary per title (designation)**

# In[157]:


q10 = spark.sql("""
            SELECT
                t.title,
                avg(Salary) Avg_Salary,
                max(Salary) Max_Salary,
                min(Salary) Min_Salary
            FROM salaries s
            JOIN employees e ON s.emp_no = e.emp_no
            JOIN titles t ON e.emp_title_id = t.title_id
            GROUP BY t.title
         """).toPandas()
q10.head(10)



# ## **11. Calculate employee tenure & show the tenure distribution among the employees**

# In[159]:


q11 = spark.sql("""
           SELECT
                CASE WHEN last_date IS NULL
                     THEN (SELECT year(greatest(max(last_date), max(hire_date))) FROM employees)-year(hire_date)
                ELSE year(last_date)-year(hire_date) END AS Tenure_Years,
                count(*) AS Tenure_cnt
            FROM employees
            GROUP BY Tenure_Years
            ORDER BY Tenure_cnt DESC
         """).toPandas()
q11.head()


# In[160]:


employees.groupBy('Tenure_Years').agg(
    count('emp_no').alias('Tenure_cnt'))    .orderBy(
        col('Tenure_cnt').desc()).toPandas().head()



# ## **12. Count of Employee Status (Currently working or Left)
# in different departments grouped by gender**

# In[162]:


spark.sql("""
            SELECT
                dept_name, sex,
                count(Left2) Total_Count,
                sum(CASE WHEN Left2 = 0 THEN 1 ELSE 0 END) Working_Count,
                sum(Left2) Left_Count
            FROM Employees e
            JOIN dept_emp de ON e.emp_no = de.emp_no
            JOIN departments d ON de.dept_no = d.dept_no
            GROUP BY dept_name, sex
         """).show()


# # 13. Max, Min and Avg age of Employees in diffrent departments

# In[163]:


spark.sql("""
           SELECT dept_name, min(Age), max(Age), avg(Age)
           FROM (
            SELECT
                dept_name, e.emp_no,
                    (SELECT year(greatest(max(last_date), max(hire_date))) FROM Employees)-year(birth_date) Age
            FROM employees e
            JOIN dept_emp de ON e.emp_no = de.emp_no
            JOIN departments d ON de.dept_no = d.dept_no
            )a
          GROUP BY dept_name
         """).show()


# The Age group is very uniform accross all the departments.

# # 14. Count of Employees in various titles

# In[164]:


q14 = spark.sql("""
            SELECT
                t.title,
                count(Salary) Emp_Count
            FROM salaries s
            JOIN employees e ON s.emp_no = e.emp_no
            JOIN titles t ON e.emp_title_id = t.title_id
            GROUP BY t.title
            ORDER BY Emp_Count DESC
         """).toPandas()
q14.head(10)


# # 15 Average Tenure Distribution accross Departments

# In[171]:


q15 = spark.sql("""
            SELECT
                dept_name,
                avg(Tenure_Years) Tenure
            FROM employees_at e
            JOIN dept_emp de ON e.emp_no = de.emp_no
            JOIN departments d ON de.dept_no = d.dept_no
            GROUP BY dept_name
            ORDER BY Tenure DESC
         """).toPandas()
q15.head(10)


# # 16 Average Tenure Distribution accross Titles

# In[170]:


q16 = spark.sql("""
            SELECT
                title,
                avg(Tenure_Years) Tenure
            FROM employees_at e
            JOIN titles t ON e.emp_title_id = t.title_id
            GROUP BY title
            ORDER BY Tenure DESC
         """).toPandas()
q16.head(10)


# # **Building Spark ML Model and Pipeline**

# ## **Creating Final Dataframe**

# In[42]:


# Joining employees, salaries, departments, titles
emp_tsd = employees.join(
    titles,
    employees.emp_title_id == titles.title_id).join(
        salaries,
        on='emp_no').join(
            dept_emp,
            on='emp_no').join(
                departments,
                on='dept_no').withColumnRenamed(
                    'left2',
    'left')
emp_tsd.toPandas().head(10)


# In[43]:


emp_tsd.count()


# In[44]:


dfp = emp_tsd.toPandas()


# In[45]:


conti_var_df = dfp.loc[:, (dfp.dtypes == 'float64') | (
    dfp.dtypes == 'int64') | (dfp.dtypes == 'int32')]
cat_var_df = dfp.loc[:, (dfp.dtypes == 'object')]


# ### Continous Variables

# In[46]:


def fun_describe(_):
    '''
        Calculates different metrics of Numerical column
    '''
    # Records and missing values
    n_tot = _.shape[0]
    n_count = _.count()
    n_miss = _.isna().sum()
    n_miss_perc = n_miss / n_tot

    # IQR
    q_1 = _.quantile(0.25)
    q_3 = _.quantile(0.75)
    iqr = q_3 - q_1
    lc_iqr = q_1 - 1.5 * iqr
    uc_iqr = q_3 + 1.5 * iqr

    return pd.Series(data=[
        _.dtypes, n_tot, n_count, n_miss, n_miss_perc,
        _.nunique(),
        _.sum(),
        _.mean(),
        _.std(),
        _.var(), iqr, lc_iqr, uc_iqr,
        _.min(),
        _.max(),
        _.quantile(0.01),
        _.quantile(0.05),
        _.quantile(0.1),
        _.quantile(0.25),
        _.quantile(0.5),
        _.quantile(0.75),
        _.quantile(0.90),
        _.quantile(0.95),
        _.quantile(0.99)
    ],
        index=[
        'dtype', 'tot', 'n', 'nmiss', 'miss_perc',
        'cardinality', 'sum', 'mean', 'std', 'var', 'iqr',
        'lc_iqr', 'uc_iqr', 'min', 'max', 'p1', 'p5', 'p10',
        'p25', 'p50', 'p75', 'p90', 'p95', 'p99'
    ])


conti_var_df.apply(fun_describe).T.head(50)


# Here, 'emp_no' is having numeric datatype but not a variable to be
# considered as it has maximum cardinality i.e. primary key, used for
# identification only.

# ### Categorical Variables

# In[47]:


def fun_obj_describe(_):
    '''
        Calculates different metrics of Categorical column
    '''
    # Records and missing values
    n_tot = _.shape[0]
    n_count = _.count()
    n_miss = _.isna().sum()
    n_miss_perc = n_miss / n_tot

    return pd.Series(data=[
        _.dtypes, n_tot, n_count, n_miss, n_miss_perc,
        _.nunique()
    ],
        index=[
        'dtype', 'tot', 'n', 'nmiss', 'miss_perc',
        'cardinality'
    ])


cat_var_df.apply(fun_obj_describe).T.head(50)


# Encoding columns with high cardinality may effect the model, so all of
# these categorical cols with high cardinality will be dropped later.


# Dropping these following columns since these do not seem to have any effect
# in Employee Left Status :
# 'emp_title_id','birth_date','last_date','hire_date','emp_no',
# 'title_id','dept_no','first_name','last_name'

# In[87]:


df = emp_tsd.drop(
    'emp_title_id',
    'birth_date',
    'last_date',
    'hire_date',
    'emp_no',
    'title_id',
    'dept_no',
    'first_name',
    'last_name')
df.toPandas().head(20)


# In[88]:


df.printSchema()


# ## **Data Preparation for Modeling**

# ### Label Encoding :

# #### last_performance_rating :

# In[89]:


df.select('last_performance_rating').distinct().show()


# Considering the the order as S > A > B > C > PIP, Encoding values as S =
# 5, A = 4, B = 3, C = 2, PIP = 1

# In[90]:


df = df.withColumn(
    'last_performance_rating',
    F.when(
        df.last_performance_rating == 'S',
        5).when(
            df.last_performance_rating == 'A',
            4).when(
                df.last_performance_rating == 'B',
                3).when(
                    df.last_performance_rating == 'C',
        2).otherwise(1))


# #### title :

# In[91]:


df.select('title').distinct().show()


# Considering the the order as Staff < Senior Staff < Assistant Engineer <
# Engineer < Senior Engineer < Technique Leader < Manager, Encoding values
# as Manager = 7, Technique Leader = 6, Senior Engineer = 5, Engineer = 4,
# Assistant Engineer = 3, Senior Staff = 2, Staff = 1

# In[92]:


df = df.withColumn(
    'title',
    F.when(
        df.title == 'Manager',
        7).when(
            df.title == 'Technique Leader',
            6).when(
                df.title == 'Senior Engineer',
                5).when(
                    df.title == 'Engineer',
                    4).when(
                        df.title == 'Assistant Engineer',
                        3).when(
                            df.title == 'Senior Staff',
        2).otherwise(1))


# In[93]:


df.show()


# In[94]:


# Back_Up df after Label Encoding
df_bkup = df


# ### One-Hot-Encoding :

# In[96]:


# Encoding all categorical features


# In[97]:


# create object of StringIndexer class and specify input and output column
SI_sex = StringIndexer(inputCol='sex', outputCol='sex_Index')
SI_dept_name = StringIndexer(inputCol='dept_name', outputCol='dept_name_Index')

# transform the data
df = SI_sex.fit(df).transform(df)
df = SI_dept_name.fit(df).transform(df)

# view the transformed data
df.select('sex', 'sex_Index', 'dept_name', 'dept_name_Index').show(10)


# In[98]:

# create object and specify input and output column
OHE_sex = OneHotEncoderEstimator(inputCols=['sex_Index'],outputCols=['sex_vec'])
OHE_dept_name = OneHotEncoderEstimator(inputCols=['dept_name_Index'],outputCols=['dept_name_vec'])

# transform the data
df = OHE_sex.fit(df).transform(df)
df = OHE_dept_name.fit(df).transform(df)

# view and transform the data
df.select('sex', 'sex_Index','sex_vec', \
          'dept_name', 'dept_name_Index','dept_name_vec').show(10)

# In[99]:


df = df.withColumnRenamed('left', 'label')


# In[100]:


df.toPandas().head()


# ### Assembler :

# In[64]:


assembler = VectorAssembler(inputCols=[
    'no_of_projects',
    'last_performance_rating',
    'Age',
    'Tenure_Years',
    'title',
    'salary',
    'sex_vec',
    'dept_name_vec', ], outputCol="features")


# In[65]:


data = assembler.transform(df).select('label', 'features')
data.show(10, truncate=False)


# ### **Train Test Split**

# In[66]:


train_df, test_df = data.randomSplit([0.7, 0.3], seed=42)


# ## **Model Building**

# In[67]:


# In[68]:


# Train Function to build the model and check accuacy
def train(train_, test_, classifier):
    '''
        This Function builds the model as per choosen Classifier \
            and also Calculates Accuracy, Error, Precision, Recall, F1 Values
    '''
    print(classifier)
    model = classifier.fit(train_)

    pred = model.transform(test_)

    eval_accuracy = (
        MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"))

    eval_precision = (
        MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedPrecision"))

    eval_recall = (
        MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedRecall"))

    eval_f1 = (MulticlassClassificationEvaluator
               (labelCol="label", predictionCol="prediction", metricName="f1"))

    accuracy = eval_accuracy.evaluate(pred)

    precision = eval_precision.evaluate(pred)

    recall = eval_recall.evaluate(pred)

    f_1 = eval_f1.evaluate(pred)

    print(f"""
  Accuracy  = {accuracy}
  Error     = {1-accuracy}
  Precision = {precision}
  Recall    = {recall}
  F1        = {f_1}""")

    return model, pred


# # Random Forest Classifier Model

# In[69]:


rfc = RandomForestClassifier(featuresCol="features",
                             labelCol="label",
                             numTrees=50,
                             maxDepth=5,
                             featureSubsetStrategy='onethird')


# In[70]:


model_rf, pred_rf = train(train_df, test_df, rfc)
pred_rf.show()


# ### Area under ROC (Random Forest Model)

# In[71]:


print(
    'Area under ROC on train dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(
            model_rf.transform(train_df)))
print(
    'Area under ROC on test dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(pred_rf))


# # Logistic Regression Model

# In[72]:


mlr = LogisticRegression(maxIter=10,
                         regParam=0.3,
                         elasticNetParam=0.8,
                         family="multinomial")


# In[73]:


model_lr, pred_lr = train(train_df, test_df, mlr)
pred_lr.show()


# ### Area under ROC (Logistic Regression Model)

# In[74]:


print(
    'Area under ROC on train dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(
            model_lr.transform(train_df)))
print(
    'Area under ROC on test dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(pred_lr))


# ### Saving the Models to HDFS :

# In[75]:


model_rf.write().overwrite().save("random_forest.model")


# In[76]:


model_lr.write().overwrite().save("logistic_regression.model")


# # **Pipeline**

# ## Pipeline Stages

# In[77]:


# Preparing database
df = employees.join(
    titles,
    employees.emp_title_id == titles.title_id).join(
        salaries,
        on='emp_no').join(
            dept_emp,
            on='emp_no').join(
                departments,
                on='dept_no').withColumnRenamed(
                    'left2',
                    'label').drop(
                        'emp_title_id',
                        'birth_date',
                        'last_date',
                        'hire_date',
                        'emp_no',
                        'title_id',
                        'dept_no',
                        'first_name',
    'last_name')
# Label Encoding
df_pl = df.withColumn(
    'last_performance_rating',
    F.when(
        df.last_performance_rating == 'S',
        5).when(
            df.last_performance_rating == 'A',
            4).when(
                df.last_performance_rating == 'B',
                3).when(
                    df.last_performance_rating == 'C',
                    2).otherwise(1)).withColumn(
                        'title',
                        F.when(
                            df.title == 'Manager',
                            7).when(
                                df.title == 'Technique Leader',
                                6).when(
                                    df.title == 'Senior Engineer',
                                    5).when(
                                        df.title == 'Engineer',
                                        4).when(
                                            df.title == 'Assistant Engineer',
                                            3).when(
                                                df.title == 'Senior Staff',
                            2).otherwise(1))
df_pl.show()


# In[78]:


conCols = [
'no_of_projects',
 'last_performance_rating',
 'Age',
 'Tenure_Years',
 'title',
 'salary']

catCols = ['sex', 'dept_name']


# In[79]:


# String Indexer
indexers = [StringIndexer(inputCol=column, outputCol=column+"_Index") for column in catCols ]

# One Hot Encoder Estimator
encoders = OneHotEncoderEstimator(inputCols=[i.getOutputCol() for i in indexers], \
                                  outputCols=[i.getOutputCol()+"_vec" for i in indexers])

# Vector Assembler
assembler = VectorAssembler(inputCols = encoders.getOutputCols() + conCols, outputCol = "features")

# ML Models
rfc = RandomForestClassifier(featuresCol="features",
                              labelCol="label",
                              numTrees=50,
                              maxDepth=5,
                              featureSubsetStrategy='onethird')
mlr = LogisticRegression(maxIter=10,
                         regParam=0.3,
                         elasticNetParam=0.8,
                         family="multinomial")
# Creating Pipelines
pipeline_rfc = Pipeline(stages = indexers + [encoders, assembler, rfc])
pipeline_mlr = Pipeline(stages = indexers + [encoders, assembler, mlr])


# In[80]:


train, test = df_pl.randomSplit([0.7, 0.3], seed=42)


# ## Checking Accuracy, Error, Precision, Recall, F1 Values

# In[81]:


def accuracy_check(pred):
    '''
        This Function calculates Accuracy, Error, Precision, Recall, F1 Values
    '''
    eval_accuracy = (
        MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"))

    eval_precision = (
        MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedPrecision"))

    eval_recall = (
        MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedRecall"))

    eval_f1 = (MulticlassClassificationEvaluator
               (labelCol="label", predictionCol="prediction", metricName="f1"))

    accuracy = eval_accuracy.evaluate(pred)

    precision = eval_precision.evaluate(pred)

    recall = eval_recall.evaluate(pred)

    f_1 = eval_f1.evaluate(pred)

    print(f"""
  Accuracy  = {accuracy}
  Error     = {1-accuracy}
  Precision = {precision}
  Recall    = {recall}
  F1        = {f_1}""")

    return pred


# # Random Forest Classifier Model

# In[82]:


model_rfc = pipeline_rfc.fit(train)
pred_rfc = model_rfc.transform(test)
pred_rfc = accuracy_check(pred_rfc)
pred_rfc.select('label', 'features', 'prediction').show(10)


# ### Area under ROC (Random Forest Model)

# In[83]:


print(
    'Area under ROC on train dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(
            model_rfc.transform(train)))
print(
    'Area under ROC on test dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(pred_rfc))


# # Logistic Regression Model

# In[84]:


model_mlr = pipeline_mlr.fit(train)
pred_mlr = model_mlr.transform(test)
pred_mlr = accuracy_check(pred_mlr)
pred_mlr.select('label', 'features', 'prediction').show(10)


# ### Area under ROC (Random Forest Model)

# In[85]:


print(
    'Area under ROC on train dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(
            model_mlr.transform(train)))
print(
    'Area under ROC on test dataset',
    BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(pred_mlr))


'''
    The Accuracies between the built models and the Pipeline models are very close.

    The reason behind the slight change in the accuracies is that the eariler case,
    the train & test split was performed after fitting the assembler but in case of ML pipeline,
    the assembler is inside the stages so assembler is fitting on split datasets seperately as a
    part of the pipeline.

    This is also clearly visible in the features column as well.

    So, this was a good test of the pipeline models in terms of accuracy and
    we can conclude that the ML Pipeline is working properly.
'''

# #### -End-
