
package com.chinaunicom.usql.core

import com.chinaunicom.usql.core.config.Settings.Key

package object config {
  /**
   * 用于stream query的调试，如果开启，则结构化流在读取到数据后就结束，而不会一直监听数据来源。
   */
  val DebugEnable: Key[Boolean] = Key("stream.debug").boolean
  val HiveEnable: Key[Boolean] = Key("stream.hive.enable").boolean
  val JobTemplateEnable: Key[Boolean] = Key("stream.template.enable").boolean
  val JobTemplateStartChar: Key[Char] = Key("stream.template.startChar").char
  val JobTemplateStopChar: Key[Char] = Key("stream.template.stopChar").char
}
