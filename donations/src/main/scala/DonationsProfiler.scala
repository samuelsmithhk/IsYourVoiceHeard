import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scalaj.http.Http

object DonationsProfiler {

  val enigmaResourceId: String = "97a6153c-213e-46a6-acd0-ad36f0f3aff0"

  def main(args: Array[String]): Unit = {

    val apiKey = args(0)

    val sparkConfig = new SparkConf()
      .setAppName("Donations Profiler")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(sparkConfig)

    val expendituresRDD = resourceIdToRDD(sc, apiKey, enigmaResourceId)
    println("Row Count: " + expendituresRDD.count.toString)

    val donationsRDD = expendituresRDD.filter(row => row.toLowerCase.contains("donation") || row.toLowerCase.contains("contribution"))
    println("Donations Count: " + donationsRDD.count.toString)


    val donorToRecipientsRDD = donationsRDD
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //convoluted split for csv to handle names with commas in them
      .map(donation => donation(3) -> donation(5))
      .groupByKey

    donorToRecipientsRDD.take(100).foreach(pair => println(pair._1 + " -> " + pair._2.mkString(" :: ")))


    sc.stop()
  }

  def resourceIdToRDD(sc: SparkContext, apiKey: String, resourceId: String): RDD[String] = {
    var returnableRDD: RDD[List[String]] = null

    val requestURL = "https://public.enigma.com/api/export/" + resourceId

    println("Requesting from enigma")
    val result = Http(requestURL)
      .header("Authorization", "Bearer " + apiKey)
      .asString

    println("Received from enigma")
    val resultBody: String = result.body
    println("Splitting body")
    val resultBodySplit: Array[String] = resultBody.split("\n")
    println("Converting to RDD")
    sc.parallelize(resultBodySplit.toSeq)
  }

}