from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
  conf = SparkConf().setAppName("Word Count")
  sc = SparkContext(conf = conf)

  book = sc.textFile("/user/cloudera/spark101/wordcount/book")
  words = book.flatMap(lambda x: x.split(" "))
  result = words.countByValue()

  print result;
  

