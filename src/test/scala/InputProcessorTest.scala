package org.pipe.pipeline

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.ArgumentMatchers.{eq => eqTo, _}
import org.mockito.MockitoSugar
import config.{Config, FileInput}
import inputprocessor.InputProcessor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InputProcessorTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "InputProcessor" should "process input files correctly" in {
    // Create a mock SparkSession
    val mockSpark = mock[SparkSession]

    // Create a mock DataFrame
    val mockDataFrame = mock[DataFrame]

    // Create a mock DataFrameReader and stub its behavior to return the mock DataFrame
    val mockDataFrameReader = mock[DataFrameReader]
    when(mockDataFrameReader.option("header", "true")).thenReturn(mockDataFrameReader)
    when(mockDataFrameReader.csv(any[String])).thenReturn(mockDataFrame)

    // Stub the sparkSession.read() method to return the mock DataFrameReader
    when(mockSpark.read).thenReturn(mockDataFrameReader)

    // Create a mock config object
    val mockConfig = mock[Config]
    val fileInput1 = FileInput("local", "table1", "C:\\Users\\ganas\\NewScala\\multipleclasses\\data\\file1.csv")
    val fileInput2 = FileInput("local", "table2", "C:\\Users\\ganas\\NewScala\\multipleclasses\\data\\file2.csv")
    when(mockConfig.file_input).thenReturn(List(fileInput1, fileInput2))

    // Create an instance of InputProcessor with the mocked SparkSession
    val inputProcessor = new InputProcessor(mockSpark)

    // Call the process method
    val result = inputProcessor.process(mockConfig)

    // Verify that the DataFrame was read for each file path
    verify(mockSpark.read.option("header", "true")).csv(eqTo("C:\\Users\\ganas\\NewScala\\multipleclasses\\data\\file1.csv"))
    verify(mockSpark.read.option("header", "true")).csv(eqTo("C:\\Users\\ganas\\NewScala\\multipleclasses\\data\\file2.csv"))

    // Verify that createOrReplaceTempView was called with the correct table names
    verify(mockDataFrame).createOrReplaceTempView("table1")
    verify(mockDataFrame).createOrReplaceTempView("table2")

    // Assert the result map contains the correct table names and DataFrames
    result should contain allOf(
      "table1" -> mockDataFrame,
      "table2" -> mockDataFrame
    )
  }
}
