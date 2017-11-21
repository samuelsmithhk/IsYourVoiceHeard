import java.io._
import org.apache.spark.{SparkConf, SparkContext}

object Profiler {

  def main1(args: Array[String]): Unit = {
    val donationsCSVFilePath = args(0)
    val outputPath = args(1)

    val fileWriter = new PrintWriter(new File(outputPath))

    val sparkConfig = new SparkConf()
      .setAppName("Enigma Political Donations Profiler")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(sparkConfig)

    val donationsRDD = sc
      .textFile(donationsCSVFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .filter(row => row.toLowerCase.contains("donation") || row.toLowerCase.contains("contribution"))
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //splitting csv with escaped commas
      .persist

    //number of donators

    val donatorsRDD = donationsRDD.map(_(3)).distinct
    val numberOfDonators = donatorsRDD.count

    fileWriter.write("Number of donators: " + numberOfDonators.toString)

    //number of recipients

    val recipientsRDD = donationsRDD.map(_(5)).distinct
    val numberOfRecipients = recipientsRDD.count

    fileWriter.write("\nNumber of recipients: " + numberOfRecipients.toString)

    val largestDonation = donationsRDD
      .map(_(13).toInt)
      .distinct
      .max

    fileWriter.write("\nLargest Donation: " + largestDonation.toString)
    //Donator with most recipients

    // https://stackoverflow.com/questions/26886275/how-to-find-max-value-in-pair-rdd
    val mostProlificDonor = donationsRDD
      .map(_(3) -> 1)
      .reduceByKey(_ + _)
      .max()(new Ordering[(String, Int)]() {
        override def compare(x: (String, Int), y: (String, Int)): Int =
          Ordering[Int].compare(x._2, y._2)})

    fileWriter.write("\nMost prolific donor: " + mostProlificDonor)

    //Recipient with most donations

    val mostDonatedTo = donationsRDD
      .map(_(5) -> 1)
      .reduceByKey(_ + _)
      .max()(new Ordering[(String, Int)]() {
        override def compare(x: (String, Int), y: (String, Int)): Int =
          Ordering[Int].compare(x._2, y._2)})

    fileWriter.write("\nRecipient with most donations: " + mostDonatedTo)

    donationsRDD.unpersist(false)
    fileWriter.close()

  }
}
