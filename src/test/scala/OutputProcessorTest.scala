package org.pipe.pipeline

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, DataFrameWriter, Row}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import config.{Config, FileWrite}
import outputprocessor.OutputProcessor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OutputProcessorTest extends AnyFlatSpec with Matchers with MockitoSugar with ArgumentMatchersSugar {

  "OutputProcessor" should "process file writes correctly" in {
    // Create a mock SparkSession
    val mockSpark = mock[SparkSession]

    // Create a mock DataFrame
    val mockDataFrame = mock[DataFrame]

    // Create a mock DataFrameWriter
    val mockDataFrameWriter = mock[DataFrameWriter[Row]]

    // Stub the DataFrame write method to return the mock DataFrameWriter
    when(mockDataFrame.write).thenReturn(mockDataFrameWriter)

    // Stub the DataFrameWriter methods to return the mock DataFrameWriter
    when(mockDataFrameWriter.mode(SaveMode.Overwrite)).thenReturn(mockDataFrameWriter)
    when(mockDataFrameWriter.option("header", "true")).thenReturn(mockDataFrameWriter)

    // Stub the DataFrameWriter.csv method to do nothing
    doAnswer((invocation: org.mockito.invocation.InvocationOnMock) => ()).when(mockDataFrameWriter).csv(any[String])

    // Create a mock config object
    val mockConfig = mock[Config]

    // Stub the file_write method to return a list of file writes
    val fileWrites = List(
      FileWrite("local", "table1", "path/to/output1.csv"),
      FileWrite("local", "table2", "path/to/output2.csv")
    )
    when(mockConfig.file_write).thenReturn(fileWrites)

    // Stub the spark.table method to return the mock DataFrame
    when(mockSpark.table(any[String])).thenReturn(mockDataFrame)

    // Create an instance of OutputProcessor with the mocked SparkSession
    val outputProcessor = new OutputProcessor(mockSpark)

    // Call the process method
    outputProcessor.process(mockConfig)

    // Verify that spark.table was called with the correct table names
    fileWrites.foreach { fileWrite =>
      verify(mockSpark).table(fileWrite.table_name)
    }

    // Verify that DataFrameWriter.csv was called with the correct parameters
    fileWrites.foreach { fileWrite =>
      verify(mockDataFrameWriter).csv(fileWrite.file_output)
    }
  }
}
