""" Example of passing funcitons in Chapter 3, page 30
Example 3-18 and 3-19

This code will not actually run since I do not have an exmaple log file

rdd referes to a RDD.  In this example we do not have an RDD because
of the misisng log file
"""
from pyspark import SparkConf, SparkContext

# for standalone apps, you need to create a sparkcontext (sc) first
conf = SparkConf().setMaster("local").setAppName("functions")
sc = SparkContext(appName="functions")

word = rdd.filter(lambda s: "error" in s)


def containsError(s):
    return "error in s"

word = rdd.filter(containsError)


class SearchFunctions(object):
    def __init__(self, query):
        self.query = query

    def isMatch(self, s):
        return self.query in s

    def getMatchesFunctionReference(self, rdd):
        # Problem: reference all of 'self' in 'self.isMatch'
        return rdd.filter(self.isMatch)

    def getMatchesMemberReference(self, rdd):
        # Problem: references all of 'self' in 'self.query'
        return rdd.filter(lambda x: self.query in x)

    def getMatchesNoReference(self, rdd):
        query = self.query
        return rdd.filter(lambda x: query in x)
