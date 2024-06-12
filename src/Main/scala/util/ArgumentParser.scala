package org.pipe.pipeline
package util

import org.apache.spark.sql.SparkSession

class ArgumentParser {

  val spark = SparkSession.builder()
    .appName("Spark Pipeline Example")
    .master("local[*]")
    .getOrCreate()

  def parseArguments(): Unit = {

  }

}
