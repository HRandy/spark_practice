from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
  conf = SparkConf().setAppName("Word Count")
  sc = SparkContext(conf = conf)

  book = sc.textFile("/user/hadoop4/spark101/wordcount/book")
  words = book.flatMap(lambda x: x.split(" "))
  result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
  result.saveAsTextFile("/user/hadoop4/spark101/wordcount/output")


