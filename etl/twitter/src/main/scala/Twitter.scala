import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf

object Twitter {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf()
      .setAppName("Enigma Political Donations Profiler")
      .setMaster("local[2]")

    val sc: StreamingContext = new StreamingContext(sparkConfig, Seconds(5))

    TwitterUtils.createStream(sc, None)
      .map(_.getText).print

    sc.start
    sc.awaitTermination
  }
}