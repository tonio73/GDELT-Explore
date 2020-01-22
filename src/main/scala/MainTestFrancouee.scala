object MainTestFrancouee extends App {

  import scala.collection.mutable.ArrayBuffer

  val a: String =
    """4#Toronto, Ontario, Canada#CA#CA08#43.6667#-79.4167#-574890;Ontario, France#CA#CA08#43.6667#-79.4167#-574890;
                    3#Oakland, California, United States#US#USCA#37.8044#-122.271#277566;
                    , Espagne#US#USCA#37.8044#-122.271#277566;
3#Los Angeles, California, France#FR#USCA#34.0522#-118.244#1662328;
3#Chicago, Illinois, United States#US#USIL#41.85#-87.6501#423587;
3#Washington, Washington, United States#US#USDC#38.8951#-77.0364#531871;
4#Rockies, Canada (General), Canada#CA#CA00#50#-114#-572409,4#Toronto, Ontario, Canada#CA#CA08#12686#43.6667#-79.4167#-574890#2152;
3#Los Angeles, California, United States#US#USCA#CA037#34.0522#-118.244#1662328#2075;4#Rockies, Canada (General), Canada#CA#CA00#12534#50#-114#-572409#2045;
3#Oakland, California, United States#US#USCA#CA001#37.8044#-122.271#277566#355;3#Oakland, California, United States#US#USCA#CA001#37.8044#-122.271#277566#2571;
3#Washington, Washington, United States#US#USDC#DC001#38.8951#-77.0364#531871#1210;3#Chicago, Illinois, United States#US#USIL#IL031#41.85#-87.6501#423587#2114"""

  val e: String = """|2#New York, United States#US#USNY##42.1497#-74.9384#NY#62;2#New York, United States#US#USNY##42.1497#-74.9384#NY#224;2#New York, United States#US#USNY##42.1497#-74.9384#NY#340;2#New York, United States#US#USNY##42.1497#-74.9384#NY#451;2#New York, United States#US#USNY##42.1497#-74.9384#NY#1356;2#New York, United States#US#USNY##42.1497#-74.9384#NY#2273;2#New York, United States#US#USNY##42.1497#-74.9384#NY#2481;1#America#US#US##39.828175#-98.5795#US#2579;1#France#FR#FR##46#2#FR#768;1#France#FR#FR##46#2#FR#2095;4#Vienna, Wien, Austria#AU#AU09#5672#48.2#16.3667#-1995499#699;1#United States#US#US##39.828175#-98.5795#US#820  """

  val array: Array[String] = e.split("[,;]")

  def CombinationsCountries(array: Array[String]): List[List[String]] = {
    /**
     *
     * Iteration sur les chaines de caractères de l'Array divisé (split) par ","
     * pour créer la liste des pays mensionnés dans l'article
     */

    val countries = ArrayBuffer[String]()

    for (s1: String <- array) {

      var country: String = ""

      if (s1.matches(".*#.*#.*")) {

        if (s1.split("#")(0).trim.forall(_.isDigit)) {
          country = s1.split("#")(1).trim
        }
        else {
          country = s1.split("#")(0).trim
        }

        if (countries.find(x => x == country) == None) {

          countries += country
        }
      }
    }

    val countries_list = countries.toList.combinations(2).toList

    println(countries_list)
    val sorted: Seq[List[String]] = countries_list.map(x => x.sortWith(_ < _))

    sorted.toList
  }

  println(CombinationsCountries(array))
}
