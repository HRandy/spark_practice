from pyspark import SparkConf, SparkContext

reader = None
def add_location_info(line):
  import geoip2.database
  global reader
  if reader is None:
    # should be pre-installed in work nodes
    reader = geoip2.database.Reader('/usr/local/maxmind/GeoLite2-City.mmdb')
  
  fields = line.split(",")
  ip = fields[13]
  try:
    response = reader.city(ip)
    country = response.country.name
    city = response.city.name
    lat =  response.location.latitude
    lon = response.location.longitude
  except:
    (country, city, lat, lon) = ("","","","")

  location_info_added = "%s,%s,%s,%s,%s" % (line, country, city, lat, lon)
  return location_info_added

if __name__ == "__main__":

  conf = SparkConf().setAppName("add location info")
  sc = SparkContext(conf = conf)
  # raw logs
  logs = sc.textFile("hdfs://localhost/user/cloudera/spark101/audi/data/")
  location_info_added = logs.map(add_location_info)
  # raw logs with location infomation appended
  location_info_added.saveAsTextFile("hdfs://localhost/user/cloudera/spark101/audi/location_info_added")







