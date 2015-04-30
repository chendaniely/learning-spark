from pyspark import SparkConf, SparkContext

# for standalone apps, you need to create a sparkcontext (sc) first
conf = SparkConf().setMaster("local").setAppName("my script")
sc = SparkContext(appName="my script")

# create an RDD called lines
lines = sc.textFile("README.md")

# example of an 'action'
print("This is the value of 'lines.count()' from the python script: {} ~~~~~~~"
      .format(lines.count()))

print("This is the value of 'lines.first()' from the python script: {} ~~~~~~~"
      .format(lines.first()))

# filtering
# example of a 'transformation'
pythonLines = lines.filter(lambda line: "Python" in line)
print("This is the value of 'pythonLines.first()' form the python script: {}"
      .format(pythonLines.first()))
