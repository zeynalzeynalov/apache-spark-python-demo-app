from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Simple Spark Application") \
    .getOrCreate()

# Sample data to be used
data = [("Alice", 25), ("Bob", 30), ("Cathy", 29)]

# Define a schema
columns = ["Name", "Age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
print("Initial DataFrame:")
df.show()

# Perform a transformation: Filter people older than 28
filtered_df = df.filter(df.Age > 28)

# Show the filtered DataFrame
print("Filtered DataFrame (Age > 28):")
filtered_df.show()

# Stop the SparkSession
spark.stop()