import java.io._
import org.apache.spark.{SparkConf, SparkContext}

object Profiler {

  def main(args: Array[String]): Unit = {
    val membersFilePath = args(0)
    val motionsFilePath = args(1)
    val rollCallFilePath = args(2)
    val outputPath = args(3)

    val fileWriter = new PrintWriter(new File(outputPath))

    val sparkConfig = new SparkConf()
      .setAppName("House of Representatives Roll Call Profiler")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(sparkConfig)

    // For members, pull out the member names, and store in RDD for later
    val membersFileRDD = sc
      .textFile(membersFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //splitting csv with escaped commas
      .persist

    val memberFullNamesRDD = membersFileRDD.map(row => row(0))
    fileWriter.write("Members fullnames: \n\n")

    val memberFullNames = memberFullNamesRDD.collect()

    memberFullNames.foreach(name => fileWriter.write(name + "\n"))

    // Count number of "states" (delegates of non-states should be obvious here)
    val statesRDD = membersFileRDD.map(row => row(1)).distinct
    val stateCount = statesRDD.count()

    fileWriter.write("\nNumber of states: " + stateCount.toString + "\n\n")

    // State with most districts? How many at-large districts are there?
    val districtsRDD = membersFileRDD
      .map(row => row(1) -> row(2))
      .groupByKey
      .persist

    val numberOfDistrictsPerStateRDD = districtsRDD.map(entry => entry._1 -> entry._2.size)

    // https://stackoverflow.com/questions/26886275/how-to-find-max-value-in-pair-rdd
    val biggestState = numberOfDistrictsPerStateRDD
      .max()(new Ordering[(String, Int)]() {
        override def compare(x: (String, Int), y: (String, Int)): Int =
          Ordering[Int].compare(x._2, y._2)})

    fileWriter.write("Biggest state: " + biggestState + "\n\n")

    val atLargeStatesRDD = districtsRDD.filter(entry => entry._2.iterator.next.equals("At Large")).persist
    val numberOfAtLargeStates = atLargeStatesRDD.count
    val atLargeStates = atLargeStatesRDD.collect

    fileWriter.write("Number of at-large states: " + numberOfAtLargeStates.toString + "\nAnd they are:\n")
    atLargeStates.foreach(entry => fileWriter.write(entry._1 + "\n"))

    atLargeStatesRDD.unpersist(false)
    districtsRDD.unpersist(false)

    //For the motions, how many are lacking a description
    val motionsRDD = sc
      .textFile(motionsFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))

    val missingDescriptionsRDD = motionsRDD
      .filter(motion => motion(2).equals(""))
      .map(motion => motion(0))

    val rollCallsWithMissingDescriptions = missingDescriptionsRDD.collect()

    fileWriter.write("Number of roll calls with missing descriptions: "
      + rollCallsWithMissingDescriptions.length.toString + "\n And they are:\n")
    rollCallsWithMissingDescriptions.foreach(rollcall => fileWriter.write(rollcall + "\n"))


    //For roll call, how many types of answers can there be?
    val rollCallRDD = sc.textFile(rollCallFilePath)
      .mapPartitionsWithIndex((index, iterator) => if (index == 0) iterator.drop(1) else iterator)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)) //splitting csv with escaped commas
      .persist

    val answersRDD = rollCallRDD
      .map(answer => answer(2) -> 1)
      .reduceByKey(_ + _)

    val answers = answersRDD.collect()

    fileWriter.write("\nWhat kind of votes can members cast?\n")
    answers.foreach(answer => fileWriter.write(answer._1 + ": " + answer._2 + "\n"))

    membersFileRDD.unpersist(false)
    fileWriter.close()
  }

}