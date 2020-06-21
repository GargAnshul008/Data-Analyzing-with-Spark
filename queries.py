#!/usr/bin/env python

def basic_query(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a dataframe corresponding to the
    first five people, ordered alphabetically by last_name, first_name.

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/bm106/pub/people_small.csv`

    schema : string
        The CSV schema
    '''

    # This loads the CSV file with proper header decoding and schema
    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    top5 = spark.sql('SELECT * FROM people ORDER BY last_name, first_name ASC LIMIT 5')

    return top5

# --- ADD YOUR NEW QUERIES BELOW ---
#
def csv_avg_income(spark, file_path):
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')
    q1 = spark.sql('SELECT zipcode,avg(income)  FROM people GROUP BY zipcode ')

    return q1
def csv_max_income(spark, file_path):
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')
    q2 = spark.sql('SELECT last_name,max(income)  FROM people GROUP BY last_name ')

    return q2
def csv_sue(spark, file_path):
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')
    q3 = spark.sql('SELECT *  FROM people where first_name="Sue" and income >=75000 ')

    return q3


def pq_avg_income(spark, file_path):
    people = spark.read.parquet(file_path)
                            

    people.createOrReplaceTempView('people')
    q1 = spark.sql('SELECT zipcode,avg(income)  FROM people GROUP BY zipcode') 

    return q1
def pq_max_income(spark, file_path):
    people = spark.read.parquet(file_path)


    people.createOrReplaceTempView('people')
    q1 = spark.sql('SELECT last_name,avg(income)  FROM people GROUP BY last_name ')
    return q1
def pq_sue(spark, file_path):
    people = spark.read.parquet(file_path)


    people.createOrReplaceTempView('people')
    q1 = spark.sql('SELECT * FROM people where first_name="Sue" and income >=75000 ')
    return q1
