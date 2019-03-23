package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.{RandomElements, Spark, TopicNamer, Wiring}
import com.ubiquibit.buoy._
import com.ubiquibit.buoy.serialize.DefSer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

class ReportFreqFromBeginning(env: {
  val stationRepository: StationRepository
  val spark: Spark
}) extends RandomElements with TopicNamer {

  private val Log: Logger = Logger.getLogger(getClass.getName)

  val repo: StationRepository = env.stationRepository
  val ss: SparkSession = env.spark.session
  val sc: SparkContext = env.spark.sc

  private val conf: Config = ConfigFactory.load()

  def run(): Unit = {

    val result = for (staId <- randomElemOf(repo.readStations()))
      yield (staId.toString, staId.feeds.find(f => f._2 == DONE))

    //    if (result.isDefined && result.get._2.isDefined) {

    //      val stationId = StationId.makeStationId(result.get._1)
    val stationId = StationId.makeStationId("EPTT2")
    val feedType = Text // result.get._2.get._1

    val topic: String = topicName(stationId, feedType)

    import ss.implicits._
    import org.apache.spark.sql.functions._

    // TODO pull out decoder/deserializer logic to allow loading by BuoyData feed type
    val enc: Encoder[TextRecord] = Encoders.product[TextRecord]
    ss.udf.register("deserialize", (bytes: Array[Byte]) => {
      DefSer.deserialize(bytes).asInstanceOf[TextRecord] }
    )

    val ds = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val valueOnly = ds
      .selectExpr("value AS value")
      .select("value")

    val recordDF = valueOnly
        .selectExpr(s"deserialize(value) AS `$stationId $feedType record`")

    val cout = recordDF.writeStream
      .format("console")
      .option("truncate", "false")
      .start()

    cout.awaitTermination()

    //    }
    //    else log.info("No stations active at this time.")

  }

}

object ReportFreqFromBeginning {
  def main(args: Array[String]): Unit = {
    Wiring.reportFreqFromBeginning.run()
  }
}
