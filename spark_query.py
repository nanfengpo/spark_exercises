from __future__ import print_function
import subprocess
from pyspark import SparkContext, SparkConf

def getConf(name):
	conf = (SparkConf()
	.set("spark.app.name", name)
	.set("spark.master", "spark://10.0.1.86:7077")
	.set("spark.driver.memory", "1g")
	.set("spark.eventLog.enabled", "true")
	.set("spark.eventLog.dir", "/home/ubuntu/storage/logs")
	.set("spark.executor.memory", "21g")
	.set("spark.executor.cores", "4")
	.set("spark.task.cpus", "1"))
	return conf

def stringToKeyValue_Int(line):
	cols = line.split("|")
	return (int(cols[1]),int(cols[2]))
	
def stringToKeyValue_String(line):
	cols = line.split("|")
	return (int(cols[0]),cols[1])

def clearOutputFiles(path):
	subprocess.call("hadoop fs -rm -r -f " + path ,shell=True)

if __name__ == "__main__":
	name = "CS-838-Assignment2-Question3"
	numOfResults = 5
	homePath = "/user/ubuntu/"
	productTablePath = homePath + "cs838/prj2/q3/product"
	salesTablePath = homePath +"cs838/prj2/q3/SALES.txt"
	
	sc = SparkContext(conf=getConf(name))
	
	salesTable = sc.textFile(salesTablePath)
	productsTable = sc.textFile(productTablePath)
	
	clearOutputFiles(homePath + name)
	
	salesRDD = salesTable.map(stringToKeyValue_Int)
	salesPerProduct = salesRDD.reduceByKey(lambda a, b: a + b)
	sortedSalesPerProduct = sc.parallelize(salesPerProduct.sortBy(lambda x: x[1], False).take(numOfResults))
	#print(sortedSalesPerProduct.collect())
	
	productsRDD = productsTable.map(stringToKeyValue_String)
	resultsRDD = sortedSalesPerProduct.join(productsRDD).sortBy(lambda x: x[1][0], False)
	#Saving to File
	resultsRDD.map(lambda x: x[1][1]).coalesce(1).saveAsTextFile(homePath + name)
	
	#print(resultsRDD.collect())
	sc.stop()
