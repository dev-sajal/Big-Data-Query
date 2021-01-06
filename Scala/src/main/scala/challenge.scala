//import all necessary packages
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object etljob{
  //Configure all files that will be used during execution
  val input_path = "file:///home/labuser/Desktop/Project/wings-vgsales-challenge/input/"
  val output_path = "file:///home/labuser/Desktop/Project/wings-vgsales-challenge/output/"
  val filename = "vgsales.csv"
  val final_file = "vgsales_data"
  val report1 = "sales_by_genre"
  val report2 = "sports_max_sales"

  //Configure the schema to be used
  val schema = StructType(Array(StructField("Name", StringType),
                      StructField("Platform",  StringType),
                      StructField("Year",  StringType),
                      StructField("Genre",  StringType),
                      StructField("Publisher",  StringType),
                      StructField("NA_Sales",  FloatType),
                      StructField("EU_Sales",  FloatType),
                      StructField("JP_Sales",  FloatType),
                      StructField("Other_Sales",  FloatType))
  )
  //Create spark session
  val spark = SparkSession
                .builder
                .master("local")
                .appName("video game sales")
                .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  
  def main(args: Array[String]){

    //Start execution of ETL pipeline 
    //--------------------------------------------------------------------//
    //NOTE: Please refer to problem statements from challenge.htm file    //
    //found under Instruction folder for detailed requirements. Function  //
    //skeletons are defined for you for each Problem. You are expected to //
    //complete them as per the instructions from challege.htm             //  
    //--------------------------------------------------------------------//

    //PROBLEM 1 - Extract the input csv data from local storage into a dataframe.
    //input_path and filename are already set for you
    val data = extract_data(spark,input_path,filename)

    //If you want to print data to terminal, uncomment below before execution 
    //data.show(5)
    //data.printSchema()
    
    //PROBLEM 2: TRANSFORM
    //Transform: Columns ‘Year’ and ‘Publisher’ have some bad records in form of value “N/A”. 
    val transform_1 = clean_NA(data)

    //PROBLEM 3: TRANSFORM
    //Create a new column called “Global_Sales” as per the rules given from challenge.htm. 
    val transform_2 = create_globalsales(transform_1)
    
    //Assign final transformed file data_transformed
    val data_transformed = transform_2
    
    //If you want to print data to terminal, uncomment below before execution
    //data_transformed.show(5)

    //PROBLEM 4: ANALYTICS
    //Create a new report to show the total global sales for each genre as per the rules given from challenge.htm.
    val sales_by_genre = sales_genre(data_transformed)

    //If you want to print data to terminal, uncomment below before execution
    //sales_by_genre.show(5)
    
    //PROBLEM 5 - ANALYTICS
    //Create a report to understand which platform has highest global level sales for sports as per the rules given from challenge.htm.
    
    val sport_max_sales = sports_maxsale(data_transformed)
    
    //If you want to print data to terminal, uncomment below before execution
    //sport_max_sales.show(5)
    

    //PROBLEM 6 - LOAD
    //Load/Save the dataframes from transformation and analytics step above to
    //local storage as Parquet files. output_path and file names are already set for you.
    
    load_data(data_transformed,output_path,final_file)
    load_data(sales_by_genre,output_path,report1)
    load_data(sport_max_sales,output_path,report2)


    //Stop spark session
    spark.stop()

  }

  def extract_data(spark: SparkSession,input_path: String ,filename: String): DataFrame = 
  {
    /** This function is to extract data into a dataframe
    * @param spark: SparkSession
    * @param input_path: Path of the input file
    * @param filename: Name of the input file
    * @return a new Dataframe
    */
    //Write your code below this line
    val path = input_path + filename
    val df = //Your code goes here
    return df
    //Write your code above this line
    
  }

  def clean_NA(data: DataFrame): DataFrame = 
  {
    /** This function is to clean the bad records in form of value “N/A”.
    * @param data: Dataframe
    * @return a new Dataframe
    */

    //Write your code below this line
    val df = //Your code goes here
    return df
    //Write your code above this line
  }

  def create_globalsales(data: DataFrame): DataFrame = 
  {
    /** This function is to Create a new column called “Global_Sales”
    *
    * @param data: Dataframe
    * @return a new Dataframe
    */

    //Write your code below this line
    val df = //Your code goes here
    return df
    //Write your code above this line
  }

  def sales_genre(data: DataFrame): DataFrame = 
  {
    /** This function is to calculate the total sales by genre
    *
    * @param data: Dataframe
    * @return a new Dataframe
    */

    //Write your code below this line
    val df = //Your code goes here
    return df
    //Write your code above this line
  }

  def sports_maxsale(data: DataFrame): DataFrame = 
  {
    /** This function is to calculate the highest global level sales for sports for each of the platforms. 
    *
    * @param data: Dataframe
    * @return a new Dataframe
    */

    //Write your code below this line
    val df = //Your code goes here
    return df
    //Write your code above this line
  }

  def load_data(data_to_load: DataFrame,output_path: String,filename: String) = 
  {
    /** This function is to load the data based on the parameters received.
    *
    * @param data_to_load: Dataframe to load
    * @param output_path: Path where the data should be loaded
    * @param filename: The name of the output file
    */

    val path = output_path + filename
    //Write your code below this line
    
    //Write your code above this line
  }
}