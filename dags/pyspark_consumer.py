from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import Normalizer, StopWordsRemover
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.linalg import SparseVector
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.ml import Pipeline

spark = SparkSession \
    .builder \
    .appName("Movie Recommendation Consumer") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("title", StringType()),
    StructField("overview", StringType()),
])

df_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tmdb_movie_data") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

tokenizer = Tokenizer(inputCol="overview", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashing_tf = HashingTF(inputCol="filtered_words", outputCol="tf_features", numFeatures=10000)
idf = IDF(inputCol="tf_features", outputCol="features")
normalized_features = Normalizer(inputCol="features", outputCol="norm_features", p=2.0)

pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, normalized_features])
model = pipeline.fit(df_raw)
featured_movies = model.transform(df_raw)

cosine_similarity = udf(lambda x, y: float(x.dot(y)), FloatType())

# Replace the target_movie_id with the movie ID for which you want to find similar movies
target_movie_id = 100

target_movie = featured_movies.filter(col("id") == target_movie_id).select(col("norm_features").alias("target_features"))

similar_movies = featured_movies.alias("m1").crossJoin(target_movie.alias("m2")) \
    .withColumn("similarity", cosine_similarity(col("m1.norm_features"), col("m2.target_features"))) \
    .select("m1.id", "m1.title", "similarity") \
    .orderBy(col("similarity").desc()) \
    .limit(10)

query = similar_movies \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
