package org.pipe.pipeline
package inputprocessor

import config.Config

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.pipeline.multipleclasses.main.getClass

import java.util.logging.Logger

class InputProcessor(spark: SparkSession) {
  val log = Logger.getLogger(getClass.getName)

  def process(config: Config): Map[String, DataFrame] = {
    config.file_input.map { fileInputIterator =>
      log.info("---File Read Start - " + fileInputIterator.table_name + "---")
      val df = spark.read.option("header", "true").csv(fileInputIterator.path)

      df.createOrReplaceTempView(fileInputIterator.table_name)
      log.info(s"Temporary view created: $fileInputIterator.table_name")
      log.info("---File Read End - " + fileInputIterator.table_name + "---")
      fileInputIterator.table_name -> df
    }.toMap
  }
}
