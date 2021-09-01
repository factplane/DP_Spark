from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, DateType, IntegerType, FloatType
from pyspark.sql.functions import udf
from pyspark.sql.functions import when


# Creating spark context
spark = SparkSession.builder.master("local[4]").appName('Weather').getOrCreate()


def getDataFrame(filepath, skiprows):
    n_skip_rows = skiprows
    row_rdd = spark.sparkContext.textFile(filepath).zipWithIndex().filter(lambda row: row[1] >= n_skip_rows).map(lambda row: row[0])
    schema = StructType([
        StructField("Dummy", StringType(), True),
        StructField("Date", DateType(), True),
        StructField("Minimum_temperature_C", FloatType(), True),
        StructField("Maximum_temperature_C", FloatType(), True),
        StructField("Rainfall_mm", IntegerType(), True),
        StructField("Evaporation_mm", FloatType(), True),
        StructField("Sunshine_hours", FloatType(), True),
        StructField("Direction_of_maximum_wind_gust", StringType(), True),
        StructField("Speed_of_maximum_wind_gust_kmh", FloatType(), True),
        StructField("Time_of_maximum_wind_gust", StringType(), True),
        StructField("9am_Temperature_C", FloatType(), True),
        StructField("9am_relative_humidity_pct", IntegerType(), True),
        StructField("9am_cloud_amount_oktas", IntegerType(), True),
        StructField("9am_wind_direction", StringType(), True),
        StructField("9am_wind_speed_kmh", IntegerType(), True),
        StructField("9am_MSL_pressure_hPa", FloatType(), True),
        StructField("3pm_Temperature_C", FloatType(), True),
        StructField("3pm_relative_humidity_pct", IntegerType(), True),
        StructField("3pm_cloud_amount_oktas", IntegerType(), True),
        StructField("3pm_wind_direction", StringType(), True),
        StructField("3pm_wind_speed_kmh", IntegerType(), True),
        StructField("3pm_MSL_pressure_hPa", FloatType(), True)])

    columns = ["Dummy",
               "Date",
               "Minimum_temperature_C",
               "Maximum_temperature_C",
               "Rainfall_mm",
               "Evaporation_mm",
               "Sunshine_hours",
               "Direction_of_maximum_wind_gust",
               "Speed_of_maximum_wind_gust_kmh",
               "Time_of_maximum_wind_gust",
               "9am_Temperature_C",
               "9am_relative_humidity_pct",
               "9am_cloud_amount_oktas",
               "9am_wind_direction",
               "9am_wind_speed_kmh",
               "9am_MSL_pressure_hPa",
               "3pm_Temperature_C",
               "3pm_relative_humidity_pct",
               "3pm_cloud_amount_oktas",
               "3pm_wind_direction",
               "3pm_wind_speed_kmh",
               "3pm_MSL_pressure_hPa"]

    df = spark.read.options(inferSchema=True, header=False, delimter=",").csv(row_rdd, schema=schema)
    # print(df.printSchema())
    # print(df.show())
    return df


def isGoodDay(maxTemp, windSpeed):
    if windSpeed is None:
        if maxTemp is None: return 0
        elif  (maxTemp >= 17.0) and (maxTemp <= 30.0):return 1
        else: return 0

    else:
        if maxTemp is None:
            if windSpeed <= 24.0:return 1
            else: return 0
        else:
            if (maxTemp >= 17.0) and (maxTemp <= 30.0) and (windSpeed <= 24.0):
                return 1
            else:
                return 0

goodDayUDF = udf(lambda x, y: isGoodDay(x, y), IntegerType())


def isBadDay(maxTemp, windSpeed):
    if windSpeed is None:
        if maxTemp is None: return 0
        elif (maxTemp > 35.0) or (maxTemp < 17.0):
            return 1
        else:
            return 0

    else:
        if maxTemp is None:
            if windSpeed >31.0:
                return 1
            else:
                return 0
        else:
            if (maxTemp > 35.0) or (maxTemp < 17.0) or (windSpeed >31.0):
                return 1
            else:
                return 0


badDayUDF = udf(lambda x, y: isBadDay(x, y), IntegerType())


def isAverageDay(maxTemp, windSpeed):
    if windSpeed is None:
        if maxTemp is None:
            return 0
        elif (maxTemp >= 17.0) and (maxTemp <= 35.0):
            return 1
        else:
            return 0

    else:
        if maxTemp is None:
            if (windSpeed <= 31.0) and (windSpeed >= 25.0):
                return 1
            else:
                return 0
        else:
            if (maxTemp >= 17.0) and (maxTemp <= 35.0) and (windSpeed >= 25.0) and (windSpeed <= 31.0):
                return 1
            elif (maxTemp >= 31.0) and (maxTemp <= 35.0) and (windSpeed <= 31.0):
                return 1
            else:
                return 0

averageDayUDF = udf(lambda x,y:isAverageDay(x,y),IntegerType())

df_sydney = getDataFrame("syd.csv", 10)
df_melbourne = getDataFrame("melb.csv", 10)
#df_sydney.show()
#df_melbourne.show()

df_sydney.fillna(0)
df_melbourne.fillna(0)

df_syd = df_sydney.select("Date", "Maximum_temperature_C", "Speed_of_maximum_wind_gust_kmh") \
    .withColumn("GoodDay", goodDayUDF(df_sydney.Maximum_temperature_C, df_sydney.Speed_of_maximum_wind_gust_kmh)) \
    .withColumn("BadDay", badDayUDF(df_sydney.Maximum_temperature_C, df_sydney.Speed_of_maximum_wind_gust_kmh)) \
    .withColumn("AverageDay", averageDayUDF(df_sydney.Maximum_temperature_C, df_sydney.Speed_of_maximum_wind_gust_kmh))
df_mel = df_melbourne.select("Date", "Maximum_temperature_C", "Speed_of_maximum_wind_gust_kmh") \
    .withColumn("GoodDay", goodDayUDF(df_melbourne.Maximum_temperature_C, df_melbourne.Speed_of_maximum_wind_gust_kmh)) \
    .withColumn("BadDay", badDayUDF(df_melbourne.Maximum_temperature_C, df_melbourne.Speed_of_maximum_wind_gust_kmh)) \
    .withColumn("AverageDay",averageDayUDF(df_melbourne.Maximum_temperature_C, df_melbourne.Speed_of_maximum_wind_gust_kmh))

print(df_syd.show())
print(df_mel.show())
