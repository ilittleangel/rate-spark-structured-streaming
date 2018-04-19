/**
  * Description of class/module...
  * Created by angelrojo on 2018-04-20.
  */
package bigdata.streaming.utils

import java.io.File

object RateArgumentParser extends scopt.OptionParser[RateArguments]("RateStructuredStreaming") {
  head("RateStructuredStreaming", "1.0.0")
  help("help").text("Print this usage text")
  version("version").text("Show version")

  opt[File]('c', "config")
    .required()
    .valueName("<file>")
    .action((x, c) => c.copy(conf = x))
    .validate(file =>
      if (file.isFile && file.exists) success
      else failure("Config file must exist"))
    .validate(file =>
      if (file.canRead) success
      else failure("Config file must be readable"))
    .text("Path to the configuration file")
}

case class RateArguments(conf: File = new File("."))
