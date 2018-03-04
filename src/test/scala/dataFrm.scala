

import org.apache.spark.sql.SparkSession

object dataFrm {
	def main(args: Array[String]) {


		import org.apache.log4j.{Level, Logger}
		import org.apache.spark.SparkConf
		//import org.mongodb.scala.model.Filters._
		
		
		// Set the log level to only print errors
		Logger.getLogger("org").setLevel(Level.ERROR)

		var conf = new SparkConf().setAppName("datafm").setMaster("local[2]")
		val spark = SparkSession
			.builder()
			.appName("datafm")
			.config(conf)
			.getOrCreate()

		// For implicit conversions like converting RDDs to DataFrames
		//val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		var rdd ="/Users/khyathi/IdeaProjects/Input/Sales.csv"

		val datafm = spark.read.option("header","true").option("inferSchema","true").csv(rdd)

		//datafm.describe().show()
		//datafm.printSchema()
		//datafm.head(3)
		datafm.groupBy("Sales").avg("Sales").show
		datafm.orderBy("Sales").show()

		datafm.select("Sales").show()

		//datafm.show()
		//datafm.filter("Sales < 300 AND Person == 'SAM' " ).show
		//datafm.filter("Person == 'Sam'").show
		//datafm.filter($"Sales" <300 && "Person" == "Sam").show()
		//datafm.select("Sales").show()

		//var datafm2 = datafm.withColumn("sales3",datafm("Sales")+ 2)


//		datafm2.groupBy("sales3").count().show()
	//	datafm2.groupBy()


		//var datafm3 = datafm2.filter("sales3 + 1").collect()
		//datafm2.show()
	}
}