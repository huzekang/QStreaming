
package com.chinaunicom.usql.core

import com.chinaunicom.usql.core.config.Settings.Key

package object config {
  val DebugEnable: Key[Boolean] = Key("stream.debug").boolean
  val HiveEnable: Key[Boolean] = Key("stream.hive.enable").boolean
  val JobTemplateEnable: Key[Boolean] = Key("stream.template.enable").boolean
  val JobTemplateStartChar: Key[Char] = Key("stream.template.startChar").char
  val JobTemplateStopChar: Key[Char] = Key("stream.template.stopChar").char
}
