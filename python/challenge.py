#import all necessary packages
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Configure all files that will be used during execution
input_path = 'file:///home/labuser/Desktop/Project/wings-vgsales-challenge/input/'
output_path = 'file:///home/labuser/Desktop/Project/wings-vgsales-challenge/output/'
filename = 'vgsales.csv'
final_file = "vgsales_data"
report1 = "sales_by_genre"
report2 = "sports_max_sales"

#Configure the schema to be used
schema = StructType([StructField("Name", StringType(), True),
                    StructField("Platform",  StringType(), True),
                    StructField("Year",  StringType(), True),
                    StructField("Genre",  StringType(), True),
                    StructField("Publisher",  StringType(), True),
                    StructField("NA_Sales",  FloatType(), True),
                    StructField("EU_Sales",  FloatType(), True),
                    StructField("JP_Sales",  FloatType(), True),
                    StructField("Other_Sales",  FloatType(), True)]
)

#Main program starts here
def main():

    #Create spark session
    spark = SparkSession.builder.appName("video game sales").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

	# Start execution of ETL pipeline   
    ##--------------------------------------------------------------------##
    ##NOTE: Please refer to problem statements from challenge.htm file    ##
    ##found under Instruction folder for detailed requirements. Function  ##
    ##skeletons are defined for you for each Problem. You are expected to ##
    ##complete them as per the instructions from challege.htm             ##  
    ##--------------------------------------------------------------------##
     
	# PROBLEM 1 - Extract the input csv data from local storage into a dataframe.
    # input_path and filename are already set for you
    data = extract_data(spark,input_path,filename)

    #If you want to print data to terminal, uncomment below before execution 
    data.show(5)
    data.printSchema()

    # PROBLEM 2: TRANSFORM 
    # Transform: Columns ‘Year’ and ‘Publisher’ have some bad records in form of value “N/A”. 
    transform_1 = clean_NA(data)
    
    #PROBLEM 3: TRANSFORM
    #Create a new column called “Global_Sales” as per the rules given from challenge.htm. 
    transform_2 = create_globalsales(transform_1)

    # Assign final transformed file data_transformed
    data_transformed = transform_2
    
    #If you want to print data to terminal, uncomment below before execution 
    data_transformed.show(5)

    #PROBLEM 4: ANALYTICS
    #Create a new report to show the total global sales for each genre as per the rules given from challenge.htm.
    
    sales_by_genre = sales_genre(data_transformed)
    
    #If you want to print data to terminal, uncomment below before execution 
    sales_by_genre.show(5)


    #PROBLEM 5 - ANALYTICS
    #Create a report to understand which platform has highest global level sales for sports as per the rules given from challenge.htm.
    
    sport_max_sales = sports_maxsale(data_transformed)
    
    #If you want to print data to terminal, uncomment below before execution 
    sport_max_sales.show(5)
    
      
    # PROBLEM 6 - LOAD
    #Load/Save the dataframes from transformation and analytics step above to 
    #local storage as Parquet files.
    #output_path and file names are already set for you
    load_data(data_transformed,output_path,final_file)
    load_data(sales_by_genre,output_path,report1)
    load_data(sport_max_sales,output_path,report2)

    #Stop spark session
    spark.stop()

def extract_data(spark,input_path,filename):
    """This function is to extract data into a dataframe
    
    Parameters:
    spark: SparkSession
    input_path: Path of the input file
    filename: Name of the input file

    Returns:
    New Dataframe
    """
    path = input_path + filename
    #Write your code below this line
    df = spark.read.option("header", "True").format("csv").schema(schema).load(path)
    return df
    #Write your code above this line
    

def clean_NA(data):
    """This function is to clean the bad records in form of value “N/A”.
    
    Parameters:
    data: Dataframe

    Returns:
    New Dataframe
    """
    #Write your code below this line
    df = data.fillna({"Year": "9999", "Publisher": "Unknown"})##Your code goes here
    return df
    #Write your code above this line

def create_globalsales(data):
    """This function is to Create a new column called “Global_Sales”
    
    Parameters:
    data: Dataframe

    Returns:
    New Dataframe
    """
    #Write your code below this line
    df = data.withColumn('Global_Sales', round(data.EU_Sales+data.NA_Sales+data.Other_Sales+data.JP_Sales, 2))
    return df
    #Write your code above this line

def sales_genre(data):
    """This function is to calculate the total sales by genre
    
    Parameters:
    data: Dataframe

    Returns:
    New Dataframe
    """
    #Write your code below this line
    df = data.groupby('Genre').agg(round(sum('Global_Sales'), 2).alias('sales')).sort(desc('sales'))
    return df
    #Write your code above this line

def sports_maxsale(data):
    """This function is to calculate the highest global level sales for sports for 
    each of the platforms. 
    
    Parameters:
    data: Dataframe

    Returns:
    New Dataframe
    """
    #Write your code below this line
    df = data.groupby('Platform').agg(max('Global_Sales').alias('Maxsales')).sort(desc('Maxsales'))
    return df
    #Write your code above this line

def load_data(data_to_load,output_path,filename):
    """This function is to load the data based on the parameters received.
    
    Parameters:
    data_to_load: Dataframe
    output_path: Path where the data should be loaded
    filename: the output file name

    Returns:
    New Dataframe
    """
    path = output_path + filename
    #Write your code below this line
    data_to_load.coalesce(1).write.format("parquet").mode("overwrite").save(path)
    #Write your code above this line

if __name__ == "__main__":
	main()
