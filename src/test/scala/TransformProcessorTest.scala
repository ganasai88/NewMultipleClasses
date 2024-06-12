package org.pipe.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import config.{Config, Transform}
import transforms.TransformProcessor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformProcessorTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "TransformProcessor" should "process transforms correctly" in {
    // Create a mock SparkSession
    val mockSpark = mock[SparkSession]

    // Create a mock DataFrame
    val mockDataFrame = mock[DataFrame]

    // Create an instance of TransformProcessor with the mocked SparkSession
    val transformProcessor = new TransformProcessor(mockSpark)

    // Create a mock config object
    val mockConfig = mock[Config]

    // Create mock transforms
    val transform1 = Transform("transform1", "SELECT * FROM table1")
    val transform2 = Transform("transform2", "SELECT * FROM table2")
    when(mockConfig.transform).thenReturn(List(transform1, transform2))

    // Stub the spark.sql method to return the mock DataFrame
    when(mockSpark.sql(any[String])).thenReturn(mockDataFrame)

    // Call the process method
    transformProcessor.process(mockConfig)

    // Verify that spark.sql was called with the correct SQL queries
    verify(mockSpark).sql("SELECT * FROM table1")
    verify(mockSpark).sql("SELECT * FROM table2")

    // Verify that createOrReplaceTempView was called with the correct table names
    verify(mockDataFrame).createOrReplaceTempView("transform1")
    verify(mockDataFrame).createOrReplaceTempView("transform2")
  }
}
