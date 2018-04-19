/**
  * Description of class/module...
  * Created by angelrojo on 2018-04-20.
  */
package bigdata.streaming.job

import com.typesafe.config.Config

import scala.util.Random


class StructuredStreamingJobConfig(config: Config) extends Serializable {
  private val root = config.getConfig("application")
  val appName: String = root.getString("app_name")
  val readStreamFormat: String = root.getString("input.read_stream_format")
  val partitions: String = root.getString("input.num_partitions")
  val rowsPerSeconds: String = genRandom(root)
  val rampUpTime: String = root.getString("input.ramp_up_time")
  val writeStreamFormat: String = root.getString("output.write_stream_format")
  val triggerTime: Long = root.getLong("output.trigger_time_in_seconds")
  val streamingAggregationsEnabled: Boolean = root.getBoolean("output.streaming_aggregations_enabled")
  val hdfsPath: String = root.getString("output.hdfs_path")
  val checkpointDir: String = root.getString("output.checkpoint_location")

  private def genRandom(config: Config): String = {
    val max = config.getInt("input.max_rows_per_seconds")
    val min = config.getInt("input.min_rows_per_seconds")
    (Random.nextInt(max + 1 - min) + min).toString
  }
}
