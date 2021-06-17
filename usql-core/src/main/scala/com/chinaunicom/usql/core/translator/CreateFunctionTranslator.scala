
package com.chinaunicom.usql.core.translator

import com.chinaunicom.usql.core.PipelineContext
import com.chinaunicom.usql.core.config.CreateFunctionStatement
import com.chinaunicom.usql.core.parser.{BooleanDataType, ByteDataType, DateDataType, DoubleDataType, FloatDataType, IntDataType, LongDataType, ShortDataType, SmallIntDataType, SqlStructType, StringDataType, TimeStampDataType, TinyIntDataType}
import com.chinaunicom.usql.core.source.WaterMarker
import com.chinaunicom.usql.core.udf.ScalaDynamicUDF
import com.chinaunicom.usql.util.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.util.Try

case class CreateFunctionTranslator(udfStatement: CreateFunctionStatement) extends StatementTranslator with WaterMarker with Logging {
  override def translate(context :PipelineContext): Unit = {

    val params = udfStatement.funcParam match {
      case Some(params) => params
      case None => ""
    }
    val func = s"def apply (${params}) = ${udfStatement.funcBody}"
    log.debug(s"parsing udf statement: \n${func}")
    val (fun, argumentTypes, returnType) = udfStatement.dataType match  {
      case Some(dataType)=>{
        ScalaDynamicUDF(func,toDataType(dataType))
      }
      case None=> ScalaDynamicUDF(func)
    }
    val inputTypes: Seq[DataType] = Try(argumentTypes.toSeq).getOrElse(Nil)

    def builder(e: Seq[Expression]) = ScalaUDF(fun, returnType, e, Nil, inputTypes, Some(udfStatement.funcName))

    context.sparkSession.sessionState.functionRegistry.registerFunction(new FunctionIdentifier(udfStatement.funcName), builder)

  }

  private def toDataType(sqlDataType:SqlStructType): DataType ={
    val fields = sqlDataType.fields.map(field=>{
      val dataType = field.dataType match {
        case _:ShortDataType => DataTypes.ShortType
        case _:IntDataType=> DataTypes.IntegerType
        case _:SmallIntDataType=>DataTypes.IntegerType
        case _:TinyIntDataType=>DataTypes.ShortType
        case _:LongDataType=>DataTypes.LongType
        case _:StringDataType=>DataTypes.StringType
        case _:BooleanDataType=>DataTypes.BooleanType
        case _:DateDataType=>DataTypes.DateType
        case _:TimeStampDataType=>DataTypes.TimestampType
        case _:ByteDataType=>DataTypes.ByteType
        case _:FloatDataType=>DataTypes.FloatType
        case _:DoubleDataType=>DataTypes.DoubleType
        case _=>DataTypes.StringType
      }
      DataTypes.createStructField(field.name,dataType,true)
    })
    DataTypes.createStructType(fields)
  }
}
