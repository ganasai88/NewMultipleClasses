package org.pipe.pipeline
package config

import org.json4s._
import org.json4s.native.JsonMethods._
import org.pipeline.multipleclasses.main.getClass

import java.util.logging.Logger

case class FileInput(source_type: String, table_name: String, path: String)
case class Transform(table_name: String, sql: String)
case class FileWrite(source_type: String, table_name: String, file_output: String)
case class Config(file_input: List[FileInput], transform: List[Transform], file_write: List[FileWrite])

object ConfigAndJsonParser {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val log = Logger.getLogger(getClass.getName)

  private def readConfigFile(configFilePath: String): String = {
    scala.io.Source.fromFile(configFilePath).mkString
  }

  def parseJson(jsonString: String): Config = {
    parse(jsonString).extract[Config]
  }

  def parseConfigFile(configFilePath: String): Config = {
    val jsonString = readConfigFile(configFilePath)
    parse(jsonString).extract[Config]
  }

}
