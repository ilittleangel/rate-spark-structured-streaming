/**
  * Description of class/module...
  * Created by angelrojo on 2018-04-23.
  */
package bigdata.streaming.job

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}


class TestStructuredStreamingJob extends FlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  val config: Config = ConfigFactory.parseResources("application.conf").resolve()
  val cfg: StructuredStreamingJobConfig = new StructuredStreamingJobConfig(config)


  "runJob()" should "run a `rate` Structured Streaming Job" in {

    val streamingQuery = StructuredStreamingJob.runJob(spark, cfg)

    // running 30 seconds
    streamingQuery.awaitTermination(30 * 1000L)

    val status = spark.streams.active.map(_.status)
    status.foreach(println)
  }
}
