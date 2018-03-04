import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import org.apache.log4j._
import org.slf4j._
import org.apache.spark.sql.SparkSession


object sivaso {
	def main(args: Array[String]) {


		print("Hello siva Oh")
	  var rdd ="/Users/khyathi/Desktop/weather.txt"

		val spark1 = SparkSession.builder
		  	.master("local")
			.appName("sparkSes test")
			.getOrCreate()
	//	var conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[*]")
		//val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory","1g")
		val logData= spark1.sparkContext.textFile(rdd)

		//val logData = sc.textFile(rdd).cache()
	//	val count1 = logData.count()
	//	print("count", count1)
			//test.count

		var zipcode =logData.flatMap(_.split("  "))
		//if zipcode.filter(line => line)
		zipcode.foreach(println)

		//sc.stop()
	}
}
