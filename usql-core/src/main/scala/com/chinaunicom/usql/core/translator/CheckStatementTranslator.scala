package com.chinaunicom.usql.core.translator

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.config.{SinkTable, CheckStatement}
import com.chinaunicom.usql.core.exceptions.DataQualityVerificationException
import com.chinaunicom.usql.core.sink.{BatchWriter, StreamWriter}
import org.apache.spark.sql.DataFrame

case class CheckStatementTranslator(checkStatement: CheckStatement) extends StatementTranslator {
  private val executingVerificationsMsg = s"开始执行 [%s] data quality check over dataset: %s"
  private val validationsPassedMsg = s"数据稽查 [%s] passed over dataset: %s"
  private val validationsFailedExceptionMsg = s"数据稽查 [%s] failed over dataset: %s"

  override def translate(pipelineContext: PipelineContext): Unit = {
    log.info(executingVerificationsMsg.format(checkStatement.name,checkStatement.input))
    val dataset = pipelineContext.sparkSession.table(checkStatement.input)
    val verificationCheckResult = VerificationSuite().onData(dataset).addCheck(checkStatement.check).run()
    // 将结果msg输出控制台
    logCheckResult(verificationCheckResult)
    // 将结果转成df
    val checkResultDF = checkResultsAsDataFrame(pipelineContext.sparkSession, verificationCheckResult)
    // 将df写出
    checkStatement.output.foreach(saveCheckResult(_, checkResultDF))
    // 如果检查结果不通过，就抛异常
    if (verificationCheckResult.status == CheckStatus.Error) {
      throw DataQualityVerificationException(validationsFailedExceptionMsg.format(checkStatement.name,checkStatement.input))
    }

  }

  private def logCheckResult(verificationCheckResult: VerificationResult) = {
    verificationCheckResult.status match {
      case CheckStatus.Success => log.info(validationsPassedMsg.format(checkStatement.name,checkStatement.input))
      case CheckStatus.Error => log.error(validationsFailedExceptionMsg.format(checkStatement.name,checkStatement.input))
      case CheckStatus.Warning => log.warn(validationsFailedExceptionMsg.format(checkStatement.name,checkStatement.input))
    }
  }

  private def saveCheckResult(table: SinkTable, dataFrame: DataFrame) = {
    val writer = if (table.streaming)
      new StreamWriter
    else
      new BatchWriter
    writer.write(dataFrame, table)

  }


}
