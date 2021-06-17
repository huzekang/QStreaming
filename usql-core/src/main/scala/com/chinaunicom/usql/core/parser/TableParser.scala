
package com.chinaunicom.usql.core.parser

import com.chinaunicom.usql.core.config._
import com.chinaunicom.usql.core.parser.ParserHelper.{cleanQuote, parseProperty}
import com.chinaunicom.usql.core.config
import com.chinaunicom.usql.core.config.{BucketSpec, Connector, ProcTime, RowFormat, RowTime, Schema, SchemaField, SinkTable, SourceTable}
import com.chinaunicom.usql.core.parser.SqlParser.{ConnectorSpecContext, CreateSinkTableStatementContext, CreateSourceTableStatementContext, FormatSpecContext, ProcTimeContext, RowTimeContext, SchemaSpecContext}

import scala.collection.JavaConversions._
/**
 * Table Factory create source/sink table from DSL statement
 */
object TableParser {
  private def parseConnector(ctx: ConnectorSpecContext): Connector = {
    Connector(ctx.connectorType.getText, ctx.connectProps.map(parseProperty).toMap)
  }

  private def parseFormat(ctx: FormatSpecContext): RowFormat = {
    val props = Option(ctx.property()).map(_.map(parseProperty).toMap).getOrElse(Map())
    config.RowFormat(ctx.rowFormat().getText, props)
  }

  private def parseSchema(ctx: SchemaSpecContext): Schema = {

    val schemaFields = ctx.schemaField().map(field => SchemaField(field.fieldName.getText, field.fieldType.getText))

    val timeField = Option(ctx.timeField()) match {
      case Some(procTimeContext: ProcTimeContext) =>
        Some(ProcTime(procTimeContext.fieldName.getText))
      case Some(rowTimeContext: RowTimeContext) =>
        Some(RowTime(rowTimeContext.fromField.getText, rowTimeContext.eventTime.getText, cleanQuote(rowTimeContext.delayThreadsHold.getText)))
      case _ => None
    }
    Schema(schemaFields, timeField)


  }

  /**
   * create source table from dsl statement
   *
   */
  def parseSourceTable(ctx: CreateSourceTableStatementContext): SourceTable = {
    val tableName: String = ParserHelper.parseTableIdentifier( ctx.tableIdentifier())
    val connector = parseConnector(ctx.connectorSpec())


    val format = Option(ctx.formatSpec()).map(parseFormat).getOrElse(RowFormat.json(Map("derive-schemaOption" -> "true")))

    val schemaOption = Option(ctx.schemaSpec()).map(parseSchema)

    val isStreamingTable = ctx.K_STREAM() != null

    def createStreamSourceTable: SourceTable = {
      SourceTable(name = tableName, connector = connector, schema = schemaOption, format = format)
    }

    def createBatchSourceTable: SourceTable = {
      config.SourceTable(streaming = false, tableName, connector, schemaOption, format)
    }

    if (isStreamingTable) createStreamSourceTable else createBatchSourceTable
  }

  /**
   * create sink table from dsl statement
   *
   */
  def parseSinkTable(ctx: CreateSinkTableStatementContext): SinkTable = {
    val tableName: String = ParserHelper.parseTableIdentifier(ctx.tableIdentifier())
    val tableOptions: Map[String, String] = Option(ctx.tableProperties()).map(_.property().map(parseProperty).toMap).getOrElse(Map())

    val connectors = ctx.connectorSpec().map(parseConnector)

    val format = Option(ctx.formatSpec()).map(parseFormat)

    val schema = Option(ctx.schemaSpec()).map(parseSchema)


    def createStreamSinkTable: SinkTable = {
      val partition = Option(ctx.partitionSpec()).map(_.columns.map(_.getText).toArray)
      config.SinkTable(streaming = true, tableName, schema, format, connectors, partition, None, tableOptions)
    }

    def createBatchSinkTable: SinkTable = {
      val partition = Option(ctx.partitionSpec()).map(_.columns.map(_.getText).toArray)
      val bucket = Option(ctx.bucketSpec()).map(bck => BucketSpec(bck.bucketNum.getText.toInt, bck.columns.map(_.getText)))
      config.SinkTable(streaming = false, tableName, schema, format, connectors, partition, bucket, tableOptions)
    }

    val isStreaming = ctx.K_STREAM() != null
    if (isStreaming) createStreamSinkTable else createBatchSinkTable
  }


}
