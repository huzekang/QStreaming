
package com.chinaunicom.usql.core.config

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.chinaunicom.usql.core.config.ViewType.ViewType
import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.parser.SqlStructType
import com.chinaunicom.usql.core.translator.{CreateFunctionTranslator, CreateViewTranslator, InsertStatementTranslator, PipelineTranslator, SinkTableTranslator, SourceTableTranslator, SparkSqlTranslator, CheckStatementTranslator}
import com.chinaunicom.usql.util.Logging

import scala.collection.mutable.ArrayBuffer


trait Statement extends Logging {
  def execute(context: PipelineContext)
}

class Pipeline extends Statement {

  var statements: ArrayBuffer[Statement] = ArrayBuffer()

  def sinkTable(tableName: String): Option[SinkTable] = statements.filter(_.isInstanceOf[SinkTable]).map(_.asInstanceOf[SinkTable]).find(_.name == tableName)

  override def execute(context: PipelineContext): Unit = PipelineTranslator(this).translate(context)
}

/**
 * original sql statement which supported by spark or flink
 *
 * @param sql
 */
case class SqlStatement(sql: String) extends Statement {
  override def execute(context: PipelineContext): Unit = SparkSqlTranslator(this).translate(context)
}

/**
 * any custom dsl statement should extend from this class
 */
trait DSLStatement extends Statement

object ViewType extends Enumeration {
  type ViewType = Value
  val globalView, tempView = Value
}

case class CreateViewStatement(sql: String, viewName: String, options: Map[String, String] = Map(), viewType: ViewType = ViewType.tempView) extends DSLStatement {

  override def execute(context: PipelineContext): Unit = CreateViewTranslator(this).translate(context)
}

case class InsertStatement(sql: String, sinkTable: SinkTable) extends DSLStatement {

  override def execute(context: PipelineContext): Unit = InsertStatementTranslator(this).translate(context)
}

case class CreateFunctionStatement(dataType: Option[SqlStructType] = None, funcName: String, funcParam: Option[String], funcBody: String) extends DSLStatement {
  override def execute(context: PipelineContext): Unit = CreateFunctionTranslator(this).translate(context)
}


sealed abstract class Table(props: Map[String, String] = Map()) extends Statement {

  def option(key: String): Option[String] = props.get(key)

  val updateMode: Option[String] = props.get("outputMode").orElse(props.get("saveMode")).orElse(props.get("update-mode")).orElse(props.get("updateMode"))

}


case class SourceTable(streaming: Boolean = true, name: String,
                       connector: Connector,
                       schema: Option[Schema] = None,
                       format: RowFormat,
                       props: Map[String, String] = Map())
  extends Table(props) with Serializable {

  override def execute(context: PipelineContext): Unit = SourceTableTranslator(this).translate(context)
}


case class BucketSpec(bucket: Int, columns: Seq[String])

case class SinkTable(streaming: Boolean = true, name: String,
                     schema: Option[Schema] = None,
                     format: Option[RowFormat] = None,
                     connectors: Seq[Connector] = Seq(),
                     partitions: Option[Array[String]] = None,
                     bucket: Option[BucketSpec] = None,
                     props: Map[String, String] = Map())
  extends Table(props) with Serializable {

  override def execute(context: PipelineContext): Unit = SinkTableTranslator(this).translate(context)
}


case class CheckStatement(name:String, input: String, output: Option[SinkTable], check: Check) extends Statement {

  override def execute(context: PipelineContext): Unit = CheckStatementTranslator(this).translate(context)
}


case class Assertion(operator:String,value:String) {

  def longAssertion()={
    apply(operator,value.toLong)
  }

  def doubleAssertion = {
    apply(operator,value.toDouble)
  }

  private def apply[N](operator: String, evaluate: N)(implicit ordered: N => Ordered[N]): N => Boolean = operator match {
    case "=="|"=" => _ == evaluate
    case "!=" => _ != evaluate
    case ">=" => _ >= evaluate
    case ">" => _ > evaluate
    case "<=" => _ <= evaluate
    case "<" => _ < evaluate
  }
}