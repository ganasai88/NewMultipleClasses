package org.pipe.pipeline
package template

import config.ConfigAndJsonParser
import inputprocessor.InputProcessor
import outputprocessor.OutputProcessor
import transforms.TransformProcessor

import org.apache.spark.sql.SparkSession
import org.pipeline.multipleclasses.main.getClass

import java.util.logging.Logger

class QueryExecutorTemplate {

  val log = Logger.getLogger(getClass.getName)

  def process(configFilePath: String):Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Pipeline Example")
      .master("local[*]")
      .getOrCreate()

    val configFilePath = "C:\\Users\\ganas\\NewScala\\multipleclasses\\src\\main\\scala\\config.json"

    val config = ConfigAndJsonParser.parseConfigFile(configFilePath)

    log.info("****************** Processing Input File Start *************************")
    val inputProcessor = new InputProcessor(spark)
    val tableDFs = inputProcessor.process(config)
    log.info("****************** Processing Input File End *************************")

    log.info("****************** Processing Query Start *************************")
    val transformProcessor = new TransformProcessor(spark)
    val resultDFs = transformProcessor.process(config)
    log.info("****************** Processing Query End *************************")

    log.info("****************** Output Dataframe to File Start *************************")
    val outputProcessor = new OutputProcessor(spark)
    outputProcessor.process(config)
    log.info("****************** Output Dataframe to File End *************************")

    spark.stop()

  }
}
