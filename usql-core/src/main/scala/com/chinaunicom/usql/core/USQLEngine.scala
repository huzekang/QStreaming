
package com.chinaunicom.usql.core

import com.chinaunicom.usql.core.config.Settings
import com.chinaunicom.usql.core.config.{PipelineConfig, Settings}
import scopt.OptionParser

import scala.io.Source


object USQLEngine extends App {

  val cliParser: OptionParser[PipelineConfig] = new OptionParser[PipelineConfig]("USQL") {
    head("USQL")

    opt[String]('j', "job")
      .required()
      .text("Path to the pipeline dsl file")
      .action((file, c) => c.copy(pipeline = Source.fromFile(file)))

    opt[Map[String, String]]('c',"conf").valueName("k1=v1, k2=v2...")
      .optional()
      .action((vars, c) => {
        var settings = Settings.load()
        vars.foreach{
          case (k,v)=> settings = settings.withValue(k,v )
        }
        c.copy(settings= settings)
      })
      .text("variables of pipeline dsl file")

    opt[Map[String, String]]('v',"variable").valueName("k1=v1, k2=v2...")
      .optional()
      .action((vars, c) => c.copy(jobVariables= vars))
      .text("variables of pipeline dsl file")

    help("help") text "use command line arguments to specify the configuration file path or content"
  }

  val config = cliParser.parse(args, PipelineConfig.DEFAULT).getOrElse(PipelineConfig.DEFAULT)
  PipelineRunner(config).run()
}
