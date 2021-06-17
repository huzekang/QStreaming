
package com.chinaunicom.usql.core.parser

import com.chinaunicom.usql.core.config.Assertion
import com.chinaunicom.usql.core.parser.SqlParser.{AssertionContext, PropertyContext, SelectStatementContext, TableIdentifierContext}
import org.antlr.v4.runtime.misc.Interval

object ParserHelper {

  def parseProperty(pc: PropertyContext): (String, String) = {
    val propKey = cleanQuote(pc.propertyKey.getText)
    val propValue = cleanQuote(pc.propertyValue.getText)
    propKey -> propValue
  }

  def parseSql(selectStatementContext: SelectStatementContext): String = {
    val interval = new Interval(selectStatementContext.start.getStartIndex, selectStatementContext.stop.getStopIndex)
    selectStatementContext.getStart.getInputStream.getText(interval)

  }

  def parseTableIdentifier(tableIdentifierContext: TableIdentifierContext) = {
    if (tableIdentifierContext.db != null)
      cleanQuote(tableIdentifierContext.db.getText) + "." + cleanQuote(tableIdentifierContext.table.getText)
    else
      cleanQuote(tableIdentifierContext.table.getText)
  }

  /**
   * clean quote identity
   */
  def cleanQuote(str: String): String = {
    if (isQuoted(str))
      str.substring(1, str.length - 1)
    else str
  }

  private def isQuoted(str: String) = {
    (str.startsWith("`") && str.endsWith("`")) || (str.startsWith("\"") && str.endsWith("\"")) || (str.startsWith("'") && str.endsWith("'"))
  }


  implicit def dslAssertionToAssertion(ctx: AssertionContext) = {
    if (ctx == null) None else Some(Assertion(ctx.assertionOperator().getText, ctx.value.getText))
  }

}
