package org.pipe.pipeline.outputprocessor

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.pipe.pipeline.config.Config

import java.util.logging.Logger

class OutputProcessor(spark: SparkSession) {

  val log = Logger.getLogger(getClass.getName)

  def process(config: Config): Unit = {
    config.file_write.foreach { fileWriteIterator =>
      val df = spark.table(fileWriteIterator.table_name)
      log.info("---File Write Start - " + fileWriteIterator.table_name + "---")
      df.write.mode(SaveMode.Overwrite).option("header", "true").csv(fileWriteIterator.file_output)
      log.info("---File Write End - " + fileWriteIterator.table_name + "---")
    }
  }
}
