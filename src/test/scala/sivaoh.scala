import Array._

object sivaoh {


	def main(args: Array[String]) {

		def count: String = {

			println("hello siva world")
			val x = "hello"
			return (x)
		}

		//println("/nwhere is my god",count(x:String))
		var arr = Array(1,3,4,5,6,7,0)

		for(x<- arr)
			{
				println(x)
			}

		for(x <- 0 to (arr.length-1))
			{
				println(arr(x))
			}

		val kk = "hello siva is here"

		if (kk.endsWith ("e")) {
			println(s"$kk are here" )

		}
		else println(s"$kk are not here")

		val kk3 = List(1,2,2,3,3,34,4,4,4,5,5,5)
	 for (kk2 <- kk3){
		 println(s"$kk2 i am printing ")

		 for (kk3 <- Range(1,10))
		 {
			 if (kk3 == List(2,4,8,10))
			 println(s"$kk3 printing even")
			 else
				 println(s"$kk3 not even")
		 }

	 }

		def sivaadd():Int = {

			println("hello siva with def ")
			return(0)
		}

		sivaadd()

		def bools(x1:Int) :Boolean= {

				return x1 == 2
		}

		println(bools(2))

		def sivminus(x :Int, y:Int) :Int = {

			val z = x-y
			return(z)

		}
		var z = sivminus( 9 , 4)
		println(s"$z I'm priniting the z")
	}



}
