# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

fileroot = "clinicaltrial_2023"

import os
os.environ['fileroot'] = fileroot



# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/" + fileroot + ".zip", "file:/tmp/")

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -d /tmp /tmp/$fileroot.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/" + fileroot)

# COMMAND ----------

dbutils.fs.mv("file:/tmp/"+fileroot+".csv","/FileStore/tables/",True)

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/" + fileroot +".csv")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/"+fileroot+".csv")

# COMMAND ----------

# introducing Reusability and defining Filepath

clincaltrial_2023 = ("/FileStore/tables/"+fileroot+".csv")
pharma = ('/FileStore/tables/pharma.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC Load the Clinicaltrial  CSV fILE

# COMMAND ----------

#load the csv using spark content
clinicaltrial_2023rdd = sc.textFile(clincaltrial_2023)



# COMMAND ----------

# MAGIC %md
# MAGIC #Cleaning the dataset

# COMMAND ----------

#clean  the dataset
clinicaltrial_2023rdd1 = clinicaltrial_2023rdd.map(lambda row: row.replace(',,', ''))


clinicaltrial_2023rdd1.take(3)



# COMMAND ----------

clinicaltrial_2023rdd2 =clinicaltrial_2023rdd1.map(lambda line: line.strip(',\"').split('\t')) \
    .filter(lambda line: len(line) > 1)
header = clinicaltrial_2023rdd2.first()
clinicaltrial_2023rdd3 = clinicaltrial_2023rdd2.filter(lambda k:k!=header)

clinicaltrial_2023rdd3.collect()


# COMMAND ----------

#checking our cleaned dataset
clinicaltrial_2023rdd2.collect()

# COMMAND ----------

#loading the pharma csv using the spark content
pharma_rdd = sc.textFile(pharma)

# COMMAND ----------

pharma_rdd2 = sc.textFile(pharma)\
                          .map(lambda line: line.split(','))\
                          .filter(lambda line: len(line)>1)



pharma_rdd2.take(10)

# COMMAND ----------

#filtering out the header
header = pharma_rdd2.first()
pharma_rdd3 = pharma_rdd2.filter(lambda k:k !=header)

pharma_rdd3.take(4)

# COMMAND ----------

# Apply transformations to create pharma_rdd4
pharma_rdd4 = pharma_rdd \
    .map(lambda x: x.replace('"', '')) \
    .map(lambda line: line.split(','))

# Print the first two lines of data
print(pharma_rdd4.take(4))


# COMMAND ----------

# MAGIC %md
# MAGIC # 1) The number of studies in the dataset

# COMMAND ----------

# Extract the header
header = clinicaltrial_2023rdd1.first()

# Filter out the header line from the RDD
data_without_header = clinicaltrial_2023rdd1.filter(lambda line: line != header)

# Split each line by tabs, extract study identifier, and get distinct studies
distinct_studies = data_without_header.map(lambda line: line.strip(',"').split('\t')[0]).distinct()

# Count the number of distinct studies
num_distinct_studies = distinct_studies.count()

# Print the result
print("The number of distinct studies in the dataset (excluding header) is:", num_distinct_studies)


# COMMAND ----------

# MAGIC %md
# MAGIC #2)Types of Studies and their Frequencies

# COMMAND ----------

#  clinicaltrial_2023RDD is the initial RDD
clinicaltrial_2023RDD_st = clinicaltrial_2023rdd1.map(lambda k: (k.split("\t")[10] if len(k.split("\t")) > 10 else None, 1))

# Filter out records where the split didn't produce enough fields
clinicaltrial_2023RDD_st_filtered = clinicaltrial_2023RDD_st.filter(lambda x: x[0] is not None)

# Reduce by key, sort, and take the first 4 elements
clinicaltrial_rdd_studies = clinicaltrial_2023RDD_st_filtered.reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda k: k[1], ascending=False)

# Take the first 4 elements for inspection
clinicaltrial_rdd_studies.take(4)

# COMMAND ----------

# MAGIC %md
# MAGIC #3)The top 5 conditions (from Conditions) with their frequencies.

# COMMAND ----------

from operator import add

def get_top_5_conditions(clinicaltrial_data):
    """
    This function takes clinical trial data as input and returns the top 5 conditions 
    based on frequency of occurrence.
    
    Parameters:
    clinicaltrial_data (RDD): RDD containing clinical trial data.
    
    Returns:
    list: Top 5 conditions with their respective frequencies.
    """
    top_5_conditions = clinicaltrial_data \
        .map(lambda col: col[4]) \
        .filter(lambda x: x != '') \
        .flatMap(lambda x: x.split('|')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1], ascending=False) \
        .take(5)
    
    return top_5_conditions

# clinicaltrial_2023RDD2 is your RDD
top_5_Conditions = get_top_5_conditions(clinicaltrial_2023rdd2)

# Displaying the top 5 Conditions
print(top_5_Conditions)


# COMMAND ----------

header = clinicaltrial_2023rdd1.first()
clinicaltrialsponsors_rdd = clinicaltrial_2023rdd1.filter(lambda k:k !=header)\
.map(lambda k: k.split("\t"))\
.map(lambda k: (k[6],1))
clinicaltrialsponsors_rdd.take(5)


# COMMAND ----------

#splitting by delimiter, replacing, selecting index, and mapping
pcc_rdd = pharma_rdd.map(lambda k: k.split(","))\
.map(lambda k: (k[1].replace('"', '')))\
.map(lambda k: (k,1)) 

pcc_rdd.take(4)

# COMMAND ----------

# MAGIC %md
# MAGIC #4)10 most common sponsors that are not pharmaceutical companies, al

# COMMAND ----------

#joining sponsors data with parent companies that are not pharmaceutical companies
sponsors = clinicaltrialsponsors_rdd.leftOuterJoin(pcc_rdd)\
.filter(lambda k: k[1][1]==None)\
.map(lambda k: (k[0],1))\
.reduceByKey(lambda a,b: a+b)\
.sortBy(lambda k: k[1], False)

sponsors.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #5)Plot number of completed studies for each month in 2023. 

# COMMAND ----------

year = '2023'
Completed_Studies = clinicaltrial_2023rdd2 \
    .map(lambda col: (col[3], col[-1])) \
    .filter(lambda x: x[0] == 'COMPLETED') \
    .map(lambda x: x[1]) \
    .filter(lambda x: x.split('-')[0] == year) \
    .map(lambda x: (x.split('-')[1], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortByKey()

Completed_Studies.collect()

# COMMAND ----------

def changer(month):
    month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    return month_dict.get(month, None)

changed_months = Completed_Studies.map(lambda x: changer(x[0])).collect()

print("Changed months after applying changer function:")
print(changed_months)


# COMMAND ----------

# Apply changer function and filter out None values
Completed_Studies_final = Completed_Studies.map(lambda x: (changer(x[0]), x[1])) \
    .filter(lambda x: x[0] is not None) \
    .sortBy(lambda x: x[0], ascending=True) \
    .map(lambda x: (x[0], x[1]))

# Collect the sorted months along with the counts into a list
result = Completed_Studies_final.collect()

# Print the final result
print("Completed_Studies_final:")
print(result)

# COMMAND ----------

import matplotlib.pyplot as plt

# Extract months and counts from the result
months, counts = zip(*result)

# Define all months
all_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Create a dictionary to hold counts for each month
counts_dict = dict(zip(months, counts))

# Fill in counts for missing months
complete_counts = [counts_dict.get(month, 0) for month in all_months]

# Define custom colors for each bar
custom_colors = ['skyblue', 'salmon', 'lightgreen', 'orange', 'lightblue', 'pink', 'yellow', 'lightcoral', 'lightgray', 'lavender', 'lightseagreen', 'gold']

# Create a bar plot with custom colors
plt.figure(figsize=(10, 6))
bars = plt.bar(all_months, complete_counts, color=custom_colors)

# Add labels and title
plt.xlabel('Month')
plt.ylabel('Number of Completed Studies')
plt.title('Completed Studies per Month')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Add value labels on top of each bar
for bar, count in zip(bars, complete_counts):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), count, ha='center', va='bottom')

# Display the plot
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Further Analysis
# MAGIC #Plot the number of Terminated studies in 2023

# COMMAND ----------

year = '2023'
Terminated_Studies = clinicaltrial_2023rdd2 \
    .map(lambda col: (col[3], col[-1])) \
    .filter(lambda x: x[0] == 'TERMINATED') \
    .map(lambda x: x[1]) \
    .filter(lambda x: x.split('-')[0] == year) \
    .map(lambda x: (x.split('-')[1], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortByKey()
Terminated_Studies.collect()

# COMMAND ----------

def changer(month):
    month_dict = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug', '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
    return month_dict.get(month, None)

changed_months = Terminated_Studies.map(lambda x: changer(x[0])).collect()

print("Changed months after applying changer function:")
print(changed_months)

# COMMAND ----------

# Apply changer function and filter out None values
Terminated_Studies_final = Terminated_Studies.map(lambda x: (changer(x[0]), x[1])) \
    .filter(lambda x: x[0] is not None) \
    .sortBy(lambda x: x[0], ascending=True) \
    .map(lambda x: (x[0], x[1]))

# Collect the sorted months along with the counts into a list
results = Terminated_Studies_final.collect()

# Print the final result
print("Terminated_Studies_final:")
print(results)

# COMMAND ----------

import matplotlib.pyplot as plt

# Extract months and counts from the result
months, counts = zip(*results)

# Define all months
all_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Create a dictionary to hold counts for each month
counts_dict = dict(zip(months, counts))

# Fill in counts for missing months
complete_counts = [counts_dict.get(month, 0) for month in all_months]

# Define custom colors for each bar
custom_colors = ['skyblue', 'salmon', 'lightgreen', 'orange', 'lightblue', 'pink', 'yellow', 'lightcoral', 'lightgray', 'lavender', 'lightseagreen', 'gold']

# Create a bar plot with custom colors
plt.figure(figsize=(6, 4))
bars = plt.bar(all_months, complete_counts, color=custom_colors)

# Add labels and title
plt.xlabel('Month')
plt.ylabel('Number of Terminated Studies')
plt.title('Terminated Studies per Month')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45)

# Add value labels on top of each bar
for bar, count in zip(bars, complete_counts):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), count, ha='center', va='bottom')

# Display the plot
plt.tight_layout()
plt.show()

