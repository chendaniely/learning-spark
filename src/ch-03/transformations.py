from pyspark import SparkConf, SparkContext

# for standalone apps, you need to create a sparkcontext (sc) first
conf = SparkConf().setMaster("local").setAppName("transformations")
sc = SparkContext(appName="transformations")

# using map that squares all the numbers in a RDD
nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()

for num in squared:
    print(num)

# using flatMap() multiple output elements for each input element
# in this example we break a string

lines = sc.parallelize(['hello world', 'hi'])
words = lines.flatMap(lambda line: line.split(" "))
words.first()  # returns hello

words = lines.flatMap(lambda line: line.split(" ")).collect()
for word in words:
    print(word)
