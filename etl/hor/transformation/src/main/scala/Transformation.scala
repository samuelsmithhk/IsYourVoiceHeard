import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transformation {

  def main(args: Array[String]): Unit = {
    val membersFilePath = args(0)
    val rollCallFilePath = args(1)
    val outputPath = args(2)

    val sparkConfig = new SparkConf()
      .setAppName("House of Representatives Roll Call Transformer")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(sparkConfig)

    val rollCallRDD = sc.textFile(rollCallFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //splitting csv with escaped commas

    val shortNamesKeyedRDD = rollCallRDD.map(rollCall => rollCall(0) -> (rollCall(1), rollCall(2)))


    val membersFileRDD = sc
      .textFile(membersFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //splitting csv with escaped commas

    val membersFile = membersFileRDD.collect

    val nameMapRDD = shortNamesKeyedRDD
      .map(shortNameKey => shortNameKey._1 -> getFullNameForShortName(membersFile, shortNameKey._1))
      .filter(row => !row._2.equals("~NOMAP~"))
      .distinct()

    shortNamesKeyedRDD
      .join(nameMapRDD)
      .map(row => row._2._2 + "," + row._2._1._1 + "," + row._2._1._2)
      .saveAsTextFile(outputPath)

  }

  def getFullNameForShortName(memberList: Array[Array[String]], shortName: String) : String = {

    var search = memberList
    var searchTerm = shortName.toLowerCase.trim()

    if (shortName.contains("(")) { // that means its a duplicate name, separated by state
      val state = shortName.substring(shortName.indexOf("(") + 1, shortName.indexOf(")"))
      search = memberList.filter(_(1) == state)

      searchTerm = searchTerm.substring(0, searchTerm.indexOf("(")).trim()
    }

    val results = search
      .map(fullName => fullName(0).toLowerCase.trim() -> fullName(0))
      .filter(row => containsName(row._1, searchTerm))

    if (results.length != 1) {
      "~NOMAP~"
    } else {
      results(0)._2
    }
  }

  def containsName(toSearch:String, toFind:String): Boolean = {
    val toSearchSplit = toSearch.replaceAll(",|\"", "").split(" ")
    toSearchSplit(0).equals(toFind)
  }

}
