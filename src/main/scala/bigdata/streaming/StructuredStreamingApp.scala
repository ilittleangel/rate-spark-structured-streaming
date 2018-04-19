/**
  * Description of class/module...
  * Created by angelrojo on 2018-04-20.
  */
package bigdata.streaming

import bigdata.streaming.job.{StructuredStreamingJobConfig, StructuredStreamingJob}
import bigdata.streaming.utils.{RateArgumentParser, RateArguments}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery


object StructuredStreamingApp extends App {

  val parsedArgs = RateArgumentParser
    .parse(args, RateArguments())
    .getOrElse(sys.exit(1))

  implicit val config: StructuredStreamingJobConfig = new StructuredStreamingJobConfig(ConfigFactory.parseFile(parsedArgs.conf))

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName(config.appName)
    .getOrCreate()

  val streamingQuery: StreamingQuery = StructuredStreamingJob.runJob
  streamingQuery.awaitTermination()

}
