import unittest
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class sampleTest(unittest.TestCase):
 
    @classmethod
    def setUpClass(cls):
        """
        Start Spark, define config and path to test data
        """
        
        logging.info("Logging from within setup")
        cls.spark=SparkSession \
            .builder \
            .appName("sampleTest") \
            .master("local") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        
        
    def test_sales_by_genre(self):
        
        file_name = "sales_by_genre"
        input_df = self.read_file(file_name)
        
        if input_df != None:
            actual_count = input_df.count()
            expected_sales_total = 8915
            expected_count = 12          
            try:
                actual_sales_total = input_df.select("sales").agg(sum(col("sales"))).collect()[0][0]
                actual_sales_total = int(actual_sales_total)
            except:
                self.fail("Could not find sales column. Please recheck")

            self.assertEqual(actual_count,expected_count,"Count of records does not match")
            self.assertEqual(actual_sales_total,expected_sales_total,"Total of sales does not match")
        else:
            self.fail("The required input file seems missing")

    def test_sports_max_sales(self):
        
        file_name = "sports_max_sales"
        input_df = self.read_file(file_name)
        
        if input_df != None:
            actual_count = input_df.count()
            expected_maxsales_total = 148
            expected_count = 25          
            try:
                actual_maxsales_total = input_df.select("Maxsales").agg(sum(col("Maxsales"))).collect()[0][0]
                actual_maxsales_total = int(actual_maxsales_total)
            except:
                self.fail("Could not find sales column. Please recheck")

            self.assertEqual(actual_count,expected_count,"Count of records does not match")
            self.assertEqual(actual_maxsales_total,expected_maxsales_total,"Total of sales does not match")
        else:
            self.fail("The required input file seems missing")
         
    def test_vgsales_data(self):
        
        file_name = "vgsales_data"
        input_df = self.read_file(file_name)
        
        if input_df != None:
            actual_count = input_df.count()
            expected_globalsales_total = 8915
            expected_count = 16598       
            try:
                actual_globalsales_total = input_df.select("Global_Sales").agg(sum(col("Global_Sales"))).collect()[0][0]
                actual_globalsales_total = int(actual_globalsales_total)
            except:
                self.fail("Could not find sales column. Please recheck")

            self.assertEqual(actual_count,expected_count,"Count of records does not match")
            self.assertEqual(actual_globalsales_total,expected_globalsales_total,"Total of sales does not match")
        else:
            self.fail("The required input file seems missing")

    def read_file(self,file_name):
        try:
            path = "file:///home/labuser/Desktop/Project/wings-vgsales-challenge/output/" + file_name
            df = self.spark.read.parquet(path)
            return df
        except:
            print("----------------------------------------------------------------------------------------------------------")
            print("Looks like the output directory for following file is missing.{}".format(file_name))
            print("Please check whether the output file directory name matches the directory name given in instructions.")
            print("Note that you can still go ahead and submit your test. Scoring will happen accordingly.")
            print("----------------------------------------------------------------------------------------------------------")

    @classmethod
    def tearDownClass(cls):
        """
        Stop Spark
        """
        cls.spark.stop()
        
    
    
if __name__ == "__main__":
    unittest.main()
