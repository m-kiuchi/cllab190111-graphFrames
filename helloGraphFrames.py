#
# helloworld of GraphFrames
# how to run: bin/spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 helloGraphFrames.py
#
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("helloGraphFrames") \
                    .getOrCreate()
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])
# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])
v.show()
e.show()

from graphframes import *
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
