import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transformation {

  def main(args: Array[String]): Unit = {
    val donationsCSVFilePath = args(0)
    val membersFilePath = args(1)
    val outputPath = args(2)

    val sparkConfig = new SparkConf()
      .setAppName("Donations Transformer")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(sparkConfig)

    val donationsRDD = sc
      .textFile(donationsCSVFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .filter(row => row.toLowerCase.contains("donation") || row.toLowerCase.contains("contribution"))
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //splitting csv with escaped commas

    val donationRecipientRDD = donationsRDD.map(donation => donation(3) -> donation(5))

    val membersFileRDD = sc
      .textFile(membersFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //splitting csv with escaped commas

    val congressmenFullNamesRDD = membersFileRDD.map(_(0))
    val congressmenFullNames = congressmenFullNamesRDD.collect

    val drParsedRDD = donationRecipientRDD
      .map(dr => getCongressmanName(dr._2, congressmenFullNames) -> dr._1)
      .filter(dr => dr._1 != "~NOMAP~")
      .groupByKey

    drParsedRDD.saveAsTextFile(outputPath)


  }

  def getCongressmanName(recipient: String, congressmenNames: Array[String]): String = {
    //foreach congressman name
    //if name is two words
      //then recipient must match both words
    //if name is three words
      //recipient must match 2 of three

    val recipientNoPunc = recipient.replaceAll("""[\p{Punct}|\p{Space}]""", "").toLowerCase()

    for (congressmanName <- congressmenNames) {
      val congressmanNameNoPunc = congressmanName.replaceAll("""[\p{Punct}]""", "").toLowerCase()
      val congressmanNameSplit = congressmanNameNoPunc.split(" ")

      var target = 2
      var found = 0

      if (congressmanNameSplit.length >= 3) {
        target = congressmanNameSplit.length - 1
      }

      for (partialName <- congressmanNameSplit) {
        if (partialName.length >2 ) {
          if (recipientNoPunc.contains(partialName)) {
            found += 1
          }
        }

        if (found == target) {
          return congressmanName
        }
      }
    }

    "~NOMAP~"
  }


}