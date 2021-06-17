
package com.chinaunicom.usql.core.parser

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, StreamConstraints}
import com.chinaunicom.usql.core.parser.SqlParser._
import com.chinaunicom.usql.core.config.{CreateFunctionStatement, CreateViewStatement, InsertStatement, Pipeline, SqlStatement, VerifyStatement, ViewType}
import com.chinaunicom.usql.core.parser.SqlParser.{ApproxCountDistinctConstraintContext, ApproxQuantileConstraintContext, CompleteConstraintContext, ContainedInConstraintContext, ContainsEmailConstraintContext, ContainsUrlConstraintContext, DataTypeConstraintContext, DateFormatConstraintContext, HasMaxConstraintContext, HasMaxLengthConstraintContext, HasMeanConstraintContext, HasMinConstraintContext, HasMinLengthConstraintContext, HasSumConstraintContext, IsNonNegativeConstraintContext, IsPositiveConstraintContext, PatternConstraintContext, SatisfyConstraintContext, SelectStatementContext, SizeConstraintContext, UniqueConstraintContext}
import com.chinaunicom.usql.util.Logging
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.misc.Interval

class PipelineListener extends SqlBaseListener with Logging {

  val pipeline = new Pipeline

  private def printStatement(context: ParserRuleContext): Unit = {
    val statement = {
      val interval = new Interval(context.start.getStartIndex, context.stop.getStopIndex)
      context.start.getInputStream.getText(interval)
    }
    logDebug(s"parsing statement ${statement}")
  }

  override def enterSqlStatement(ctx: SqlParser.SqlStatementContext): Unit = {
    printStatement(ctx)
    val statement = {
      val interval = new Interval(ctx.start.getStartIndex, ctx.stop.getStopIndex)
      ctx.start.getInputStream.getText(interval)
    }
    pipeline.statements += SqlStatement(statement)
  }


  override def enterCreateSourceTableStatement(ctx: SqlParser.CreateSourceTableStatementContext): Unit = {
    printStatement(ctx)
    pipeline.statements += TableParser.parseSourceTable(ctx)
  }

  override def enterCreateSinkTableStatement(ctx: SqlParser.CreateSinkTableStatementContext): Unit = {
    printStatement(ctx)
    pipeline.statements += TableParser.parseSinkTable(ctx)
  }


  override def enterCreateViewStatement(ctx: SqlParser.CreateViewStatementContext): Unit = {
    printStatement(ctx)
    import scala.collection.convert.wrapAsScala._
    val options = ctx.property().map(ParserHelper.parseProperty).toMap
    val viewType = {
      if (ctx.K_GLOBAL() != null && ctx.K_TEMPORARY() != null) {
        ViewType.globalView
      } else if (ctx.K_TEMPORARY() != null) {
        ViewType.tempView
      } else {
        ViewType.tempView
      }
    }

    pipeline.statements += CreateViewStatement(ParserHelper.parseSql(ctx.selectStatement()), ParserHelper.parseTableIdentifier(ctx.tableIdentifier()), options, viewType)
  }


  override def enterCreateFunctionStatement(ctx: SqlParser.CreateFunctionStatementContext): Unit = {
    printStatement(ctx)
    import scala.collection.JavaConverters._

    val funcBody = {
      val interval = new Interval(ctx.funcBody.start.getStartIndex, ctx.funcBody.stop.getStopIndex)
      ctx.funcBody.start.getInputStream.getText(interval)
    }
    val dataType = if (ctx.functionDataType() != null) {
      val fields = ctx.functionDataType().structField().asScala
      val fieldTypes = fields.map(field => {
        val fieldName = ParserHelper.cleanQuote(field.STRING().getText)
        val fieldType = field.fieldType().getText
        SqlField(fieldName, SqlDataType(fieldType))
      })
      Some(SqlStructType(fieldTypes.toArray))
    } else {
      None
    }
    val funcParams = Option(ctx.funcParam()).map(_.asScala.map(_.getText).mkString(","))
    pipeline.statements += CreateFunctionStatement(dataType, ctx.funcName.getText, funcParams, funcBody)
  }


  def parseSql(selectStatementContext: SelectStatementContext): String = {
    val interval = new Interval(selectStatementContext.start.getStartIndex, selectStatementContext.stop.getStopIndex)
    selectStatementContext.getStart.getInputStream.getText(interval)

  }

  override def enterInsertStatement(ctx: SqlParser.InsertStatementContext): Unit = {
    printStatement(ctx)
    val sql = ParserHelper.parseSql(ctx.selectStatement())
    val tableName = ParserHelper.parseTableIdentifier(ctx.tableIdentifier())
    val sinkTableOption = pipeline.sinkTable(tableName)
    sinkTableOption match {
      case Some(sinkTable) => {
        pipeline.statements += InsertStatement(sql, sinkTable)
      }
      case None =>
        pipeline.statements += SqlStatement(s"insert into ${tableName} ${sql}")
    }
  }

  override def enterCreateTestStatement(ctx: SqlParser.CreateTestStatementContext): Unit = {

    import ParserHelper._

    import scala.collection.JavaConverters._
    val testOptions = if (ctx.property() != null) ctx.property().asScala.map(ParserHelper.parseProperty).toMap else Map[String, String]()
    val checkLevel = testOptions.get("testLevel").map(CheckLevel.withName).getOrElse(CheckLevel.Error)
    val testName = ParserHelper.cleanQuote(ctx.testName.getText)
    val testInput = ParserHelper.cleanQuote(ctx.testDataset.getText)
    val testOutput = testOptions.get("testOutput").map(ParserHelper.cleanQuote).flatMap(pipeline.sinkTable)
    var check = new Check(checkLevel, testName)

    ctx.constraint().asScala.foreach {
      case ctx: SizeConstraintContext =>
        require(ctx.assertion() != null)
        check = check.hasSize(ctx.assertion().map(_.longAssertion()).get)
      case ctx: UniqueConstraintContext =>
        ctx.column.asScala.map(_.getText).toList match {
          case head :: Nil => check = check.isUnique(head)
          case head :: tail => check = check.isPrimaryKey(head, tail: _*)
        }
      case ctx: CompleteConstraintContext =>
        check = check.hasCompleteness(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: ContainsUrlConstraintContext =>
        check = check.containsURL(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: ContainsEmailConstraintContext =>
        check = check.containsEmail(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: ContainedInConstraintContext =>
        check = check.isContainedIn(ctx.column.getText, ctx.value.asScala.map(_.getText).toArray, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: IsNonNegativeConstraintContext =>
        check = check.isNonNegative(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: IsPositiveConstraintContext =>
        check = check.isPositive(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: SatisfyConstraintContext =>
        check = check.satisfies(ctx.predicate.getText, ctx.desc.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: DataTypeConstraintContext =>
        check = check.hasDataType(ctx.column.getText, ConstrainableDataTypes.withName(ctx.dataType.getText), ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: HasMinLengthConstraintContext =>
          check = check.hasMinLength(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: HasMaxLengthConstraintContext =>
          check = check.hasMaxLength(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: HasMinConstraintContext =>
          check = check.hasMin(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: HasMaxConstraintContext =>
          check = check.hasMax(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: HasSumConstraintContext =>
          check = check.hasSum(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: HasMeanConstraintContext =>
          check = check.hasMean(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: PatternConstraintContext =>
        check = check.hasPattern(ctx.column.getText, ctx.pattern.getText.r, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: DateFormatConstraintContext =>
        val dateFormatConstraint = StreamConstraints.dateFormatConstraint(ctx.column.getText, ctx.formatString.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
        check = check.addConstraint(dateFormatConstraint)
      case ctx: ApproxQuantileConstraintContext =>
        check = check.hasApproxQuantile(ctx.column.getText, ctx.quantile.getText.toDouble, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
      case ctx: ApproxCountDistinctConstraintContext =>
        check = check.hasApproxCountDistinct(ctx.column.getText, ctx.assertion().map(_.doubleAssertion).getOrElse(Check.IsOne))
    }

    pipeline.statements += VerifyStatement(testName, testInput, testOutput, check)
  }


}
