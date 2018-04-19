/**
  * Description of class/module...
  * Created by angelrojo on 2018-04-19.
  */
package bigdata.streaming.job

import bigdata.streaming.model.Rate
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import scala.concurrent.duration._


object StructuredStreamingJob {

  def runJob(implicit spark: SparkSession, conf: StructuredStreamingJobConfig): StreamingQuery = {

    import spark.implicits._

    val rates = spark
      .readStream
      .format(conf.readStreamFormat)
      .options(Map("rowsPerSecond" -> conf.rowsPerSeconds, "rampUpTime" -> conf.rampUpTime, "numPartitions" -> conf.partitions))
      .load
      .as[Rate]
      .transform(sleep(_, conf))

    val aggregations = if (conf.streamingAggregationsEnabled) {
      rates.groupByKey(_.value % 2).agg(typed.sum (_.value))
    } else {
      rates
    }

    val dataStreamWriter = aggregations
      .writeStream
      .format(conf.writeStreamFormat)
      .trigger(Trigger.ProcessingTime(conf.triggerTime.seconds))

    val output = conf.writeStreamFormat match {
      case "console" => dataStreamWriter.outputMode(outputMode(conf.streamingAggregationsEnabled))

      case "csv" => dataStreamWriter
        .option("checkpointLocation", conf.checkpointDir)
        .option("path", conf.hdfsPath)

      case "memory" => dataStreamWriter
        .outputMode(outputMode(conf.streamingAggregationsEnabled))
        .queryName("memorytable") // this query name will be the table name
    }

    output.start

  }

  def outputMode(enabled: Boolean): OutputMode =
    if (enabled) OutputMode.Complete() else OutputMode.Append()

  def sleep(ds: Dataset[Rate], conf: StructuredStreamingJobConfig): Dataset[Rate] = {
    List.range(1, conf.triggerTime).reverse.foreach(second => {
      println(s"Sleeping $second seconds")
      Thread.sleep(1000)
    })
    ds
  }

}
