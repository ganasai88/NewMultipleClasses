package org.pipeline.multipleclasses

import org.pipe.pipeline.template.QueryExecutorTemplate

import java.util.logging.Logger

object main {

  val log = Logger.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {

    log.info("Application started")
    val configFilePath = args(0)
//    val configFilePath = "C:\\Users\\ganas\\NewScala\\multipleclasses\\src\\main\\scala\\config.json"

    val queryExecutorTemplate = new QueryExecutorTemplate()
    queryExecutorTemplate.process(configFilePath)
  }


}
