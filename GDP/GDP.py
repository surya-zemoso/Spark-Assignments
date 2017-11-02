from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setMaster('local')
#sc = SparkContext()
#sqlContext = SQLContext(sc)


spark = SparkSession.builder.appName("GDP Analysis").config(conf=SparkConf()).getOrCreate()


# Reading gdp.csv file and create it a data frame

#for creating dataframe and loading gdp.csv file, we can use any of the below method-  
#1) df = spark.read.csv('gdp.csv',mode="DROPMALFORMED",inferSchema=True,header=True)
#2) df = spark.read.format("csv").option("header", "true").load("gdp.csv")
df = spark.read.csv('gdp.csv',mode="DROPMALFORMED",inferSchema=True,header=True)
#df.show()

#Renaming the column of data frame
df = df.withColumnRenamed("Country Name", "Country_Name")
df = df.withColumnRenamed("Country Code", "Country_Code")
df.createOrReplaceTempView("GDP_temp")

#Creating a new Dataframe sqlDf which store GDP_growth_Rate
sqlDF = spark.sql("SELECT d1.Country_Name, d1.Country_Code, d1.Year, d1.Value, (d2.Value - d1.Value)*100/d1.Value as GDP_growth_Rate FROM GDP_temp d1, GDP_temp d2 WHERE d1.Country_Code = d2.Country_Code AND d2.Year = d1.Year + 1")
#sqlDF.show()
sqlDF.createOrReplaceTempView("GDP_Rate")


# Creating a dataframe which contain Year & maximum GdP value of that year 
sqlDF1 = spark.sql("SELECT d1.Year,max(d1.GDP_growth_Rate) AS GDP_growth_Rate FROM GDP_Rate d1 GROUP BY d1.Year ORDER BY d1.Year")
sqlDF1.createOrReplaceTempView("GDP_Max_Rate")
#sqlDF1.show()

#Creating a dataframe which contain year(having maximum GDP among all Country), Country_Name & the Value of GDP
sqlDF2 = spark.sql("SELECT d1.Year, d1.Country_Name, d1.Value FROM GDP_Rate d1, GDP_Max_Rate d2 WHERE d1.Year = d2.Year AND d1.GDP_growth_Rate = d2.GDP_growth_Rate ORDER BY d1.Year")
#sqlDF2.show()

#Renaming Country_Name column to Country Name
sqlDF2 = sqlDF2.withColumnRenamed("Country_Name", "Country Name")
sqlDF2.show(100)
