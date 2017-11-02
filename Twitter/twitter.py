
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Twitter Analysis").setMaster('spark://zemoso-dell:7077')
sc = SparkContext(conf=conf)


# Read file from desktop & create RDD
allTweet = sc.textFile("/home/zemoso/Desktop/tweet.txt")

#split the twitter file by " " and create single list RDD.
splitTwit = allTweet.flatMap(lambda x: x.split(" "))
 
# extracting # tag twit from splitTwit list
tagTwit = splitTwit.filter(lambda x: x.startswith('#'))

#create tuple of  #tag list & use reduceByKey to collect all tuple by same key
tupleTwit = tagTwit.map(lambda t: (t,1)).reduceByKey(lambda x,y:x+y)

#sorting the tuple by value
sortedTwit = tupleTwit.sortBy(lambda a:-a[1])

#extracting top 100 twit 
top100 = sortedTwit.take(100)

#printing top100 twit
for i in top100:
	print(i)