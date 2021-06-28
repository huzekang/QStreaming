
package com.chinaunicom.usql.core.exceptions

case class SettingsValidationException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}

case class ParsingException(message: String) extends RuntimeException(message)

/**
 * 数据稽查无法通过的异常
 * @param message
 * @param cause
 */
case class DataQualityVerificationException(private val message: String = "",
                                            private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

