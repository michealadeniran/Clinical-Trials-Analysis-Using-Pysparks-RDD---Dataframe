# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

fileroot = "clinicaltrial_2023"

# COMMAND ----------

# introducing Reusability
clincaltrial_2023 = ("/FileStore/tables/"+fileroot+".csv")
pharma = ('/FileStore/tables/pharma.csv')

# COMMAND ----------

#using the spark content
clinicaltrial_2023RDD = sc.textFile(clincaltrial_2023)
# Display the first few lines of the RDD
for line in clinicaltrial_2023RDD.take(5):  
    print(line)


# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

# Define your own schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("Study Title", StringType(), True),
    StructField("Acronym", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Conditions", StringType(), True),
    StructField("Interventions", StringType(), True),
    StructField("Sponsor", StringType(), True),
    StructField("Collaborators", StringType(), True),
    StructField("Enrollment", StringType(), True),
    StructField("Funder Type", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Study Design", StringType(), True),
    StructField("Start", StringType(), True),
    StructField("Completion", StringType(), True)
])

# COMMAND ----------

# Remove the header row
header = clinicaltrial_2023RDD.first()
data_without_header = clinicaltrial_2023RDD.filter(lambda row: row != header)

# COMMAND ----------

# Convert RDD elements to Rows and handle schema mismatch
def parse_row(row):
    fields = row.split("\t")
    # Pad the fields to match the length of the schema
    padded_fields = fields + [None] * (len(schema.fields) - len(fields))
    return Row(*padded_fields)

# Apply the parsing function and create RDD of Row objects
rdd_rows = data_without_header.map(parse_row)

# Convert RDD to DataFrame using the custom schema
df2 = spark.createDataFrame(rdd_rows, schema)

# Show the DataFrame
df2.display()


# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Cleaning

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Assuming `df` is the DataFrame you want to clean
Clinical_trial2023clean = df2.withColumn("Completion", regexp_replace("Completion", '"', ''))


# COMMAND ----------

Clinical_trial2023New=Clinical_trial2023clean.withColumn("Completion", regexp_replace("Completion", ',', ''))

# COMMAND ----------

Newdf= Clinical_trial2023New.withColumn("id", regexp_replace("id", '"', ''))

# COMMAND ----------

Newdf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Checking for Null Values

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

#checking for null values
null_counts= Newdf.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in Newdf.columns])
null_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Loading the PHARMA CSV ,Seperating the delimeter

# COMMAND ----------

#read the csv into dataframe
pharma_df = spark.read.csv(pharma,header=True,inferSchema=True,sep=",")

pharma_df.show(2, truncate=False)

# COMMAND ----------

# List of columns to keep (without ")
columns_to_keep = [col_name for col_name in pharma_df.columns if '"' not in col_name]

# Selecting only the columns without "
pharma_df = pharma_df.select(*columns_to_keep)

# Showing the DataFrame
pharma_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1)The number of studies in the dataset. 

# COMMAND ----------

# Perform distinct count for the 'id' column
distinct_id_count = Newdf.select('id').distinct().count()

# Print the distinct count for 'id'
print("Distinct count for 'id':", distinct_id_count)


# COMMAND ----------

# MAGIC %md
# MAGIC ##(2) Type of Studies in the dataset with the frequency of Each type

# COMMAND ----------

from pyspark.sql.functions import col, desc

def show_types_of_studies(Newdf):
    """
    This function takes a PySpark DataFrame containing study data,
    filters out null values in the "type" column,
    groups by "type", counts occurrences, and orders by count in descending order.
    It then shows the resulting DataFrame.
    
    :param Newdf: PySpark DataFrame containing study data
    """
    # Filter out null values in the "type" column
    filtered_df = Newdf.filter(col("type").isNotNull())
    
    # Group by "type", count occurrences, and order by count in descending order
    types_of_studies_df = filtered_df.groupBy("type").count().orderBy(desc("count"))

    # Show the resulting DataFrame
    types_of_studies_df.show()


#  Newdf is already defined as the DataFrame containing study data
show_types_of_studies(Newdf)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3)The top 5 conditions (from Conditions) with their frequencies.

# COMMAND ----------

from pyspark.sql.functions import explode, split, desc

# Filter out null values in the "Conditions" column and explode the values
exploded_conditions_df = Newdf.where(Newdf["Conditions"].isNotNull()) \
    .select(explode(split("Conditions", "\|")).alias("Condition"))

# Group by condition and count their occurrences
condition_counts_df = exploded_conditions_df.groupBy("Condition").count()

# Sort by count in descending order
sorted_condition_counts_df = condition_counts_df.orderBy(desc("count"))

# Take the top 5 conditions
top_5_conditions_df = sorted_condition_counts_df.limit(5)

# Show the top 5 conditions with their frequencies
top_5_conditions_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Find the 10 most common sponsors that are not pharmaceutical companies, alongwith the number of clinical trials they have sponsored. 

# COMMAND ----------

from pyspark.sql.functions import col

def filter_sponsors_by_parent(Newdf, parent_column='Parent_Company', sponsor_column='Sponsor'):
    # Get the list of parent companies
    parent_list = list(pharma_df.select(parent_column).toPandas()[parent_column])

    # Apply filtering and aggregation
    filtered_df = Newdf.select(sponsor_column) \
                       .filter(~col(sponsor_column).isin(parent_list)) \
                       .groupBy(sponsor_column).count() \
                       .orderBy('count', ascending=False)

    # Show the top 10 sponsors
    filtered_df.show(10, truncate=False)

# Call the function
filter_sponsors_by_parent(Newdf)


# COMMAND ----------

# MAGIC %md
# MAGIC #5) Plot number of completed studies for each month in 2023. You need to include yourvisualization as well as a table of all the values you have plotted for each month

# COMMAND ----------

from pyspark.sql.functions import year, month, count, when
from calendar import month_name

def show_month_name(month_num):
    """
    Convert numeric month value to month name.
    """
    return month_name[month_num]

def show_monthly_counts(new_df):
    """
    Show monthly counts of completed studies with month names instead of numbers.
    """
    # We'll select only the 'Status' and 'Completion' columns
    studies_df = new_df.select('Status', 'Completion')

    # Extracting year and month from the completion date
    studies_df = studies_df.withColumn('year', year('Completion'))
    studies_df = studies_df.withColumn('month', month('Completion'))

    # Filtering only completed studies in the year 2023
    studies_df = studies_df.filter((studies_df['Status'] == 'COMPLETED') & (studies_df['year'] == 2023))

    # Grouping by year and month and counting the number of completed studies
    monthly_counts = studies_df.groupBy('month').agg(count('*').alias('completed_studies_2023')).orderBy( 'month')

    # Showing month names instead of numbers
    monthly_counts = monthly_counts.withColumn('month_name', when(monthly_counts['month'] == 1, 'January')
                                               .when(monthly_counts['month'] == 2, 'February')
                                               .when(monthly_counts['month'] == 3, 'March')
                                               .when(monthly_counts['month'] == 4, 'April')
                                               .when(monthly_counts['month'] == 5, 'May')
                                               .when(monthly_counts['month'] == 6, 'June')
                                               .when(monthly_counts['month'] == 7, 'July')
                                               .when(monthly_counts['month'] == 8, 'August')
                                               .when(monthly_counts['month'] == 9, 'September')
                                               .when(monthly_counts['month'] == 10, 'October')
                                               .when(monthly_counts['month'] == 11, 'November')
                                               .when(monthly_counts['month'] == 12, 'December')
                                               .otherwise('Unknown'))

    return monthly_counts


# COMMAND ----------

result_df = show_monthly_counts(Newdf)
result_df.show(truncate=False)


# COMMAND ----------

import plotly.express as px

# Assuming result_df is your DataFrame with monthly counts and month names
months = result_df.select('month_name').rdd.flatMap(lambda x: x).collect()
counts_2023 = result_df.select('completed_studies_2023').rdd.flatMap(lambda x: x).collect()

# Create a Plotly bar plot
fig = px.bar(x=months, y=counts_2023, color=months,
             labels={'x': 'Month', 'y': 'Number of Completed Studies'},
             title='Monthly Counts of Completed Studies in 2023',
             color_discrete_sequence=px.colors.qualitative.Pastel)

# Customize layout
fig.update_layout(xaxis_tickangle=-45, yaxis_gridcolor='lightgrey')

# Show the plot
fig.show()



# COMMAND ----------

# MAGIC %md
# MAGIC # further Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #Top sponsors with their frequencies

# COMMAND ----------

from pyspark.sql.functions import desc

# Newdf is our DataFrame containing the sponsors column
# Group by sponsor and count their occurrences
sponsor_counts_df = Newdf.groupBy("Sponsor").count()

# Sort by count in descending order
sorted_sponsor_counts_df = sponsor_counts_df.orderBy(desc("count"))

# Show the top 5 sponsors with their frequencies
display(sorted_sponsor_counts_df.limit(5))



