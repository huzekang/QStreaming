
package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.config.SqlStatement
import com.chinaunicom.usql.util.Logging

case class SparkSqlTranslator(sqlStatement: SqlStatement) extends StatementTranslator with Logging {
  override def translate(context: PipelineContext): Unit = {
    var sql = sqlStatement.sql
    sql = sql.replace("@original", "").trim.toUpperCase
    logDebug(s"execute spark sql:\n ${sql}")
    // judge if need to show dataframe
    if (sql.startsWith("SELECT") || sql.startsWith("SHOW") || sql.startsWith("DESC")) {
      context.sparkSession.sql(sql).show()
    }else{
      context.sparkSession.sql(sql)
    }


  }
}
