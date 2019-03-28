package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.{RandomElements, Spark, TopicNamer, Wiring}
import com.ubiquibit.buoy._
import com.ubiquibit.buoy.serialize.DefSer
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Minute
import org.apache.spark.sql.expressions.scalalang.typed
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class StationFeed(stationId: String, feedType: String)

/**
  * Processes [[com.ubiquibit.buoy.WxStation]] station data.
  */
class WxStream(env: {
  val spark: Spark
}) extends Deserializer with RandomElements with TopicNamer {

  @transient val ss: SparkSession = env.spark.session
  @transient val sc: SparkContext = env.spark.sc
  private val conf: Config = ConfigFactory.load()

  @transient private val Log: Logger = Logger.getLogger(getClass.getName)

  import WxStream._

  def run(): Unit = {

    import ss.implicits._
    import org.apache.spark.sql.Encoders._
    import org.apache.spark.sql.functions._

    val enc: Encoder[StationFeed] = Encoders.product[StationFeed]

    SparkSession.setActiveSession(ss)

    val topics = ss.read
      .option("header", false)
      .schema(schema = stationFeedSchema)
      .csv(path = conf.getString("stage.dir"))
      .as(enc)
      .map(sf => topicName(StationId.makeStationId(sf.stationId), WxFeed.valueOf(sf.feedType).get))
      .selectExpr("value AS topic")
      .select('topic)

    val topicString = topics.map(t => t.getString(0))
      .collect()
      .mkString(",")
    Log.info(s"Reading topics: $topicString")

    val kafkaFeed = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
      .option("subscribe", topicString)
      .option("startingOffsets", "earliest")
      .option("spark.sql.shuffle.partitions", conf.getString("spark.partitions"))
      .load()

    val recordStream = kafkaFeed
      .map(deserialize)
      .withWatermark("eventTime", "12 hours")
      .groupBy(window($"eventTime", "1 hour", "15 minutes"), 'stationId, 'eventTime)
      .agg('eventTime)

    recordStream.printSchema()

    // (Flat)MapGroupsWithState

    val debugOut = recordStream
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
      .start

    debugOut.awaitTermination()

  }

}

object WxStream extends TopicNamer {

  def nameForFeed(stationFeed: StationFeed): Option[(String, WxFeed)] = {
    val feed: Option[WxFeed] = WxFeed.valueOf(stationFeed.feedType)
    if (feed.isDefined)
      Some((stationFeed.stationId, feed.get))
    else None
  }

  val stationFeedSchema: StructType = StructType(
    StructField("stationId", StringType, false) ::
      StructField("feedType", StringType, false) :: Nil
  )

  def deserialize(row: Row): TextRecord = {
    DefSer.deserialize(row.getAs[Array[Byte]]("value")).asInstanceOf[TextRecord]
  }

  def main(args: Array[String]): Unit = {
    Wiring.wxStream.run()
  }

}