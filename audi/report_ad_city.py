from pyspark import SparkConf, SparkContext

def log_stats(iter):
  (count_imp, count_click) = (0, 0)
  imei_imp = set()
  imei_click = set()
  for line in iter:
    record = line.split(",")
    (log_type, imei) = record[1], record[3]
    if log_type == 1:
      count_imp = count_imp + 1
      imei_imp.add(imei)
    else:
      count_click = count_click + 1
      imei_click.add(imei)
  return (count_imp, len(imei_imp), count_click, len(imei_click))


if __name__ == "__main__":
  conf = SparkConf().setAppName("report ad")
  sc = SparkContext(conf = conf)
  # load data
  report_input = sc.textFile("/user/cloudera/spark101/audi/report_input/")
  # transformations
  report_input_kv  = report_input.map(lambda x: ((x.split(",")[2],x.split(",")[-2],x.split(",")[-1]), x))
  grouped = report_input_kv.groupByKey()
  result = grouped.map(lambda (x,y): (x,log_stats(y)))
  # action
  for row in result.collect():
    print row
