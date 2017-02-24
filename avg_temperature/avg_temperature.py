from pyspark import SparkConf, SparkContext

def is_good(record):
  try:
    temp = int(record.split(",")[10])
  except ValueError:
    return False
  return True

if __name__ == "__main__":  
  conf = SparkConf().setAppName("Average Temperature")
  sc = SparkContext(conf = conf)

  records = sc.textFile("/user/cloudera/spark101/avg_temperature/weather")
  good_records = records.filter(is_good)
  day_temp = good_records.map(lambda x: (x.split(",")[1],int(x.split(",")[10])))

  result = day_temp.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda (x,y): (x, y[0]/y[1]))
  for line in result.collect():
    print line





