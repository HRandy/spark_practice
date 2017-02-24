from pyspark import SparkConf, SparkContext

def remove_unused_fields(line):
  fields = line.split(",")
  create_time = fields[1]
  log_type = fields[3]
  ad_id = fields[4]
  imei = fields[15]
  country = fields[-4]
  city = fields[-3]
  return "%s,%s,%s,%s,%s,%s" % (create_time, log_type, ad_id, imei, country, city)

if __name__ == "__main__":
  conf = SparkConf().setAppName("make report input")
  sc = SparkContext(conf = conf)

  location_info_added  = sc.textFile("hdfs://localhost/user/cloudera/spark101/audi/location_info_added/")
  report_input = location_info_added.map(remove_unused_fields)
  report_input.saveAsTextFile("hdfs://localhost/user/cloudera/spark101/audi/report_input")







