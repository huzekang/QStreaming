
package com.chinaunicom.usql.core.parser

import java.io.StringWriter

import com.chinaunicom.usql.core.config.Pipeline
import com.chinaunicom.usql.core.config.{Pipeline, PipelineConfig}
import freemarker.cache.StringTemplateLoader
import freemarker.template.{Configuration, TemplateExceptionHandler}
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.antlr.v4.runtime.{CharStream, CharStreams, CommonTokenStream}

import scala.collection.JavaConverters._

class PipelineParser(pipelineConfig: PipelineConfig) {

  def parse(): Pipeline = {
    parseFromString(pipelineConfig.pipeline.mkString)
  }

  def parseFromString(string: String): Pipeline = {
    val charStream = CharStreams.fromString(template(string))
    parse(charStream)
  }

  private def parse(charStream: CharStream): Pipeline = {
    val parser = new SqlParser(new CommonTokenStream(new SqlLexer(charStream)))
    val listener = new PipelineListener
    ParseTreeWalker.DEFAULT.walk(listener, parser.sql())
    listener.pipeline
  }


  private def template(template: String) = {
    val cfg = new Configuration(Configuration.VERSION_2_3_23)
    cfg.setDefaultEncoding("UTF-8")
    val stringLoader = new StringTemplateLoader
    stringLoader.putTemplate("pipeline", template)
    cfg.setTemplateLoader(stringLoader)
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER)
    val freeMarkTemplate = cfg.getTemplate("pipeline")
    val writer = new StringWriter()
    freeMarkTemplate.process(pipelineConfig.jobVariables.asJava, writer)
    writer.getBuffer.toString
  }

}


