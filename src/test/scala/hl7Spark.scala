

object hl7Spark {


	def main(args: Array[String]) {


		import org.apache.log4j.{Level, Logger}
		import org.apache.spark.SparkConf
		import org.apache.spark.sql.SparkSession
		import org.apache.spark.sql.functions._

		// Set the log level to only print errors
		Logger.getLogger("org").setLevel(Level.ERROR)

		var conf1 = new SparkConf().setAppName("datafm").setMaster("local[2]")
		val spark1 = SparkSession
			.builder()
			.appName("datafm")
			.config(conf1)
			.getOrCreate()

		var rdd = "/Users/khyathi/IdeaProjects/Input/Netflix.csv"
		//var rdd ="/Users/khyathi/IdeaProjects/Input/containsNull.csv"
		val datafm = spark1.read.option("header", "true").option("inferSchema", "true").csv(rdd)
		datafm.printSchema()
		datafm.head(2)

	}
}
