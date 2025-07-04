/*
 * Copyright 2015 TouchType Ltd
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.spark_redshift_community.spark.redshift.test

import java.io.{ByteArrayInputStream, OutputStreamWriter}
import java.net.URI
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{BucketLifecycleConfiguration, S3Object, S3ObjectInputStream}
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule
import io.github.spark_redshift_community.spark.redshift.{Parameters, Utils}
import io.github.spark_redshift_community.spark.redshift.{TableName, DefaultSource, DefaultRedshiftWriter}
import io.github.spark_redshift_community.spark.redshift.{RedshiftRelation, RedshiftWriter}
import io.github.spark_redshift_community.spark.redshift.Parameters.{MergedParameters, PARAM_USER_QUERY_GROUP_LABEL}
import io.github.spark_redshift_community.spark.redshift.{Conversions, Parameters, RowEncoderUtils}
import io.github.spark_redshift_community.spark.redshift.test.InMemoryS3AFileSystem
import org.apache.http.client.methods.HttpRequestBase
import org.mockito.ArgumentMatchers.{anyString, endsWith}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.UnsupportedFileSystemException
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should._
import org.apache.spark.SparkContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.exceptions.TestFailedException

import java.util
import scala.util.matching.Regex

/**
  * Tests main DataFrame loading and writing functionality
  */
class RedshiftSourceSuite
  extends QueryTest
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _

  private var testSqlContext: SQLContext = _

  private var expectedDataDF: DataFrame = _

  private var mockS3Client: AmazonS3Client = _

  private var s3FileSystem: FileSystem = _

  private val s3TempDir: String = "s3a://" + InMemoryS3AFileSystem.BUCKET + "/temp-dir/"

  private var unloadedData: String = ""

  // Parameters common to most tests. Some parameters are overridden in specific tests.
  // New parameters added during autopushdown enhancements are turned off to restore
  // original behavior when unit tests were written.
  private def defaultParams: Map[String, String] = Map(
    "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
    "tempdir" -> s3TempDir,
    "dbtable" -> "test_table",
    "forward_spark_s3_credentials" -> "false",
    "aws_iam_role" -> "fake_role_arn",
    Parameters.PARAM_AUTO_PUSHDOWN -> "false",
    Parameters.PARAM_PUSHDOWN_S3_RESULT_CACHE -> "false",
    Parameters.PARAM_UNLOAD_S3_FORMAT -> "TEXT"
  )

  private val queryGroupPattern: Regex = "set query_group to .*".r

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "RedshiftSourceSuite")
    sc.hadoopConfiguration.set("fs.s3a.impl", classOf[InMemoryS3AFileSystem].getName)
    // We need to use a DirectOutputCommitter to work around an issue which occurs with renames
    // while using the mocked S3 filesystem.
    sc.hadoopConfiguration.set("spark.sql.sources.outputCommitterClass",
      classOf[DirectMapreduceOutputCommitter].getName)
    sc.hadoopConfiguration.set("mapred.output.committer.class",
      classOf[DirectMapredOutputCommitter].getName)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    s3FileSystem = FileSystem.get(new URI(s3TempDir), sc.hadoopConfiguration)
    testSqlContext = new SQLContext(sc)
    testSqlContext.sql("RESET spark.datasource.redshift.community.trace_id")
    expectedDataDF =
      testSqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)

    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    mockS3Client = Mockito.mock(classOf[AmazonS3Client], Mockito.RETURNS_SMART_NULLS)

    when(mockS3Client.getBucketLifecycleConfiguration(anyString())).thenReturn(
      new BucketLifecycleConfiguration().withRules(
        new Rule().withPrefix("").withStatus(BucketLifecycleConfiguration.ENABLED)
      ))

    val mockManifest = Mockito.mock(classOf[S3Object], Mockito.RETURNS_SMART_NULLS)

    when(mockManifest.getObjectContent).thenAnswer {
      new Answer[S3ObjectInputStream] {
        override def answer(invocationOnMock: InvocationOnMock): S3ObjectInputStream = {
          val manifest =
            s"""
               | {
               |   "entries": [
               |     { "url": "${Utils.fixS3Url(Utils.lastTempPathGenerated)}/part-00000" }
               |    ]
               | }
            """.stripMargin
          // Write the data to the output file specified in the manifest:
          val out = s3FileSystem.create(new Path(s"${Utils.lastTempPathGenerated}/part-00000"))
          val ow = new OutputStreamWriter(out.getWrappedStream)
          ow.write(unloadedData)
          ow.close()
          out.close()
          val is = new ByteArrayInputStream(manifest.getBytes("UTF-8"))
          new S3ObjectInputStream(
            is,
            Mockito.mock(classOf[HttpRequestBase], Mockito.RETURNS_SMART_NULLS))
        }
      }
    }

    when(mockS3Client.getObject(anyString(), endsWith("manifest"))).thenReturn(mockManifest)
    when(mockS3Client.doesObjectExist(anyString(), endsWith("manifest"))).thenReturn(true)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    testSqlContext = null
    expectedDataDF = null
    mockS3Client = null
    FileSystem.closeAll()
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

  ignore("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    // scalastyle:off
    unloadedData =
      """
        |1|t|2015-07-01|1234152.12312498|1.0|42|1239012341823719|23|Unicode's樂趣|2015-07-01 00:00:00.001
        |1|f|2015-07-02|0|0.0|42|1239012341823719|-13|asdf|2015-07-02 00:00:00.0
        |0||2015-07-03|0.0|-1.0|4141214|1239012341823719||f|2015-07-03 12:34:56
        |0|f||-1234152.12312498|100000.0||1239012341823719|24|___\|_123|
        |||||||||@NULL@|
      """.stripMargin.trim
    // scalastyle:on
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\", \"testdate\", \"testdouble\"," +
        " \"testfloat\", \"testint\", \"testlong\", \"testshort\", \"teststring\", " +
        "\"testtimestamp\" " +
        "FROM \"PUBLIC\".\"test_table\" '\\) " +
        "TO '.*' " +
        "WITH CREDENTIALS 'aws_iam_role=fake_role_arn' " +
        "ESCAPE").r
    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped("test_table").toString -> TestUtils.testSchema))

    // Assert that we've loaded and converted all data in the test file
    val source = new DefaultSource((_, _) => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams)
    val df = testSqlContext.baseRelationToDataFrame(relation)

    checkAnswer(df, TestUtils.expectedData)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - Can load output of Redshift queries") {
    // scalastyle:off
    val expectedJDBCQuery =
      """
        |UNLOAD \('SELECT "testbyte", "testbool" FROM
        |  \(select testbyte, testbool
        |    from test_table
        |    where teststring = \\'\\\\\\\\Unicode\\'\\'s樂趣\\'\) '\)
      """.stripMargin.lines.map(_.trim).toArray.mkString(" ").trim.r
    val query =
      """select testbyte, testbool from test_table where teststring = '\\Unicode''s樂趣'"""
    unloadedData = "1|t"
    // scalastyle:on
    val querySchema =
      StructType(Seq(StructField("testbyte", ByteType), StructField("testbool", BooleanType)))

    val expectedValues = Array(Row(1.toByte, true))

    // Test with dbtable parameter that wraps the query in parens:
    {
      val params = defaultParams + ("dbtable" -> s"($query)")
      val mockRedshift =
        new MockRedshift(defaultParams("url"), Map(params("dbtable") -> querySchema))
      val relation = new DefaultSource((_, _) => mockS3Client).createRelation(testSqlContext, params)
      assert(testSqlContext.baseRelationToDataFrame(relation).collect() === expectedValues)
      mockRedshift.verifyThatConnectionsWereClosed()
      mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedJDBCQuery))
    }

    // Test with query parameter
    {
      val params = defaultParams - "dbtable" + ("query" -> query)
      val mockRedshift = new MockRedshift(defaultParams("url"), Map(s"($query)" -> querySchema))
      val relation = new DefaultSource((_, _) => mockS3Client).createRelation(testSqlContext, params)
      assert(testSqlContext.baseRelationToDataFrame(relation).collect() === expectedValues)
      mockRedshift.verifyThatConnectionsWereClosed()
      mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedJDBCQuery))
    }
  }

  ignore("DataAPI Refactoring TODO - DefaultSource supports simple column filtering") {
    // scalastyle:off
    unloadedData =
      """
        |1|t
        |1|f
        |0|
        |0|f
        ||
      """.stripMargin.trim
    // scalastyle:on
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" FROM \"PUBLIC\".\"test_table\" '\\) " +
        "TO '.*' " +
        "WITH CREDENTIALS 'aws_iam_role=fake_role_arn' " +
        "ESCAPE").r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource((_, _) => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)
    val resultSchema =
      StructType(Seq(StructField("testbyte", ByteType), StructField("testbool", BooleanType)))

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
      .mapPartitions { iter =>
        val fromRow = RowEncoderUtils.expressionEncoderForSchema(resultSchema).resolveAndBind().
          createDeserializer().apply(_)
        iter.asInstanceOf[Iterator[InternalRow]].map(fromRow)
      }
    val prunedExpectedValues = Array(
      Row(1.toByte, true),
      Row(1.toByte, false),
      Row(0.toByte, null),
      Row(0.toByte, false),
      Row(null, null))
    assert(rdd.collect() === prunedExpectedValues)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - DefaultSource supports user schema, pruned and filtered scans") {
    // scalastyle:off
    unloadedData = "1|t"
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" " +
        "FROM \"PUBLIC\".\"test_table\" " +
        "WHERE \"testbool\" = true " +
        "AND \"teststring\" = ''Unicode\\\\'\\\\'s樂趣'' " +
        "AND \"testdouble\" > 1000.0 " +
        "AND \"testdouble\" < 1.7976931348623157E308 " +
        "AND \"testfloat\" >= 1.0::float4 " +
        "AND \"testint\" <= 43'\\) " +
        "TO '.*' " +
        "WITH CREDENTIALS 'aws_iam_role=fake_role_arn' " +
        "ESCAPE").r
    // scalastyle:on
    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped("test_table").toString -> TestUtils.testSchema))

    // Construct the source with a custom schema
    val source = new DefaultSource((_, _) => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)
    val resultSchema =
      StructType(Seq(StructField("testbyte", ByteType), StructField("testbool", BooleanType)))

    // Define a simple filter to only include a subset of rows
    val filters: Array[Filter] = Array(
      EqualTo("testbool", true),
      // scalastyle:off
      EqualTo("teststring", "Unicode's樂趣"),
      // scalastyle:on
      GreaterThan("testdouble", 1000.0),
      LessThan("testdouble", Double.MaxValue),
      GreaterThanOrEqual("testfloat", 1.0f),
      LessThanOrEqual("testint", 43))
    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), filters)
      .mapPartitions { iter =>
        val fromRow = RowEncoderUtils.expressionEncoderForSchema(resultSchema).resolveAndBind().
          createDeserializer().apply(_)
        iter.asInstanceOf[Iterator[InternalRow]].map(fromRow)
      }

    assert(rdd.collect() === Array(Row(1, true)))
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - DefaultSource supports SSE-KMS key clause") {
    // scalastyle:off
    unloadedData =
      """
        |1|t
        |1|f
        |0|
        |0|f
        ||
      """.stripMargin.trim
    // scalastyle:on
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" FROM \"PUBLIC\".\"test_table\" '\\) " +
        "TO '.*' " +
        "WITH CREDENTIALS 'aws_iam_role=fake_role_arn' " +
        "ESCAPE MANIFEST NULL AS '@NULL@' KMS_KEY_ID 'abc-123' ENCRYPTED").r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithKms = defaultParams + ("sse_kms_key" -> "abc-123")
    val relation = source.createRelation(testSqlContext, paramsWithKms, TestUtils.testSchema)
    val resultSchema =
      StructType(Seq(StructField("testbyte", ByteType), StructField("testbool", BooleanType)))

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
      .mapPartitions { iter =>
        val fromRow = RowEncoderUtils.expressionEncoderForSchema(resultSchema).
          resolveAndBind().createDeserializer().apply(_)
        iter.asInstanceOf[Iterator[InternalRow]].map(fromRow)
      }
    val prunedExpectedValues = Array(
      Row(1.toByte, true),
      Row(1.toByte, false),
      Row(0.toByte, null),
      Row(0.toByte, false),
      Row(null, null))
    assert(rdd.collect() === prunedExpectedValues)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - DefaultSource includes extraunloadoptions") {
    // scalastyle:off
    unloadedData =
      """
        |1|t
        |1|f
        |0|
        |0|f
        ||
      """.stripMargin.trim
    // scalastyle:on
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" FROM \"PUBLIC\".\"test_table\" '\\) " +
        "TO '.*' " +
        "WITH CREDENTIALS 'aws_iam_role=fake_role_arn' " +
        "ESCAPE MANIFEST NULL AS '@NULL@' KMS_KEY_ID 'abc-123' ENCRYPTED  CLEANPATH").r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithKms = defaultParams + ("sse_kms_key" -> "abc-123")
    val paramsWithExtraUnloadOptions = paramsWithKms + ("extraunloadoptions" -> "CLEANPATH")
    val relation = source.createRelation(
      testSqlContext, paramsWithExtraUnloadOptions, TestUtils.testSchema)
    val resultSchema =
      StructType(Seq(StructField("testbyte", ByteType), StructField("testbool", BooleanType)))

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
      .mapPartitions { iter =>
        val fromRow = RowEncoderUtils.expressionEncoderForSchema(resultSchema).
          resolveAndBind().createDeserializer().apply(_)
        iter.asInstanceOf[Iterator[InternalRow]].map(fromRow)
      }
    val prunedExpectedValues = Array(
      Row(1.toByte, true),
      Row(1.toByte, false),
      Row(0.toByte, null),
      Row(0.toByte, false),
      Row(null, null))
    assert(rdd.collect() === prunedExpectedValues)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedQuery))
  }

  ignore("DefaultSource supports preactions options to run queries before running COPY command") {
    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped("test_table").toString -> TestUtils.testSchema))
    val source = new DefaultSource((_, _) => mockS3Client)
    val params = defaultParams ++ Map(
      "preactions" ->
        """
          | DELETE FROM %s WHERE id < 100;
          | DELETE FROM %s WHERE id > 100;
          | DELETE FROM %s WHERE id = -1;
        """.stripMargin.trim,
      "usestagingtable" -> "true")

    val expectedCommands = Seq(
      "DROP TABLE IF EXISTS \"PUBLIC\".\"test_table.*\"".r,
      "CREATE TABLE IF NOT EXISTS \"PUBLIC\".\"test_table.*\"".r,
      "DELETE FROM \"PUBLIC\".\"test_table.*\" WHERE id < 100".r,
      "DELETE FROM \"PUBLIC\".\"test_table.*\" WHERE id > 100".r,
      "DELETE FROM \"PUBLIC\".\"test_table.*\" WHERE id = -1".r,
      "COPY \"PUBLIC\".\"test_table.*\"".r)

    source.createRelation(testSqlContext, SaveMode.Overwrite, params, expectedDataDF)
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
    mockRedshift.verifyThatConnectionsWereClosed()
  }

  ignore("DefaultSource serializes data as Avro, then sends Redshift COPY command") {
    val params = defaultParams ++ Map(
      "postactions" -> "GRANT SELECT ON %s TO jeremy",
      "diststyle" -> "KEY",
      "distkey" -> "testint")

    val expectedCommands = Seq(
      "DROP TABLE IF EXISTS \"PUBLIC\"\\.\"test_table.*\"".r,
      ("CREATE TABLE IF NOT EXISTS \"PUBLIC\"\\.\"test_table.*" +
        " DISTSTYLE KEY DISTKEY \\(testint\\).*").r,
      "COPY \"PUBLIC\"\\.\"test_table.*\"".r,
      "GRANT SELECT ON \"PUBLIC\"\\.\"test_table\" TO jeremy".r)

    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped("test_table").toString -> TestUtils.testSchema))

    val relation = RedshiftRelation(
      mockRedshift.redshiftWrapper,
      (_, _) => mockS3Client,
      Parameters.mergeParameters(params),
      userSchema = None)(testSqlContext)
    relation.asInstanceOf[InsertableRelation].insert(expectedDataDF, overwrite = true)

    // Make sure we wrote the data out ready for Redshift load, in the expected formats.
    // The data should have been written to a random subdirectory of `tempdir`. Since we clear
    // `tempdir` between every unit test, there should only be one directory here.
    assert(s3FileSystem.listStatus(new Path(s3TempDir)).length === 1)
    val dirWithAvroFiles = s3FileSystem.listStatus(new Path(s3TempDir)).head.getPath.toUri.toString
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(dirWithAvroFiles)
    checkAnswer(written, TestUtils.expectedDataWithConvertedTimesAndDates)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  test("Cannot write table with column names that become ambiguous under case insensitivity") {
    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped("test_table").toString -> TestUtils.testSchema))

    val schema = StructType(Seq(StructField("a", IntegerType), StructField("A", IntegerType)))
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val writer = new RedshiftWriter(mockRedshift.redshiftWrapper, (_, _) => mockS3Client)

    intercept[IllegalArgumentException] {
      writer.saveToRedshift(
        testSqlContext, df, SaveMode.Append, Parameters.mergeParameters(defaultParams))
    }
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatCommitWasNotCalled()
    mockRedshift.verifyThatRollbackWasCalled()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  ignore("Failed copies are handled gracefully when using a staging table") {
    val params = defaultParams ++ Map("usestagingtable" -> "true")

    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped("test_table").toString -> TestUtils.testSchema),
      jdbcQueriesThatShouldFail = Seq("COPY \"PUBLIC\".\"test_table.*\"".r))

    val expectedCommands = Seq(
      "DROP TABLE IF EXISTS \"PUBLIC\".\"test_table.*\"".r,
      "CREATE TABLE IF NOT EXISTS \"PUBLIC\".\"test_table.*\"".r,
      "COPY \"PUBLIC\".\"test_table.*\"".r,
      ".*FROM stl_load_errors.*".r
    )

    val source = new DefaultSource((_, _) => mockS3Client)
    intercept[Exception] {
      source.createRelation(testSqlContext, SaveMode.Overwrite, params, expectedDataDF)
    }
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatCommitWasNotCalled()
    mockRedshift.verifyThatRollbackWasCalled()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  ignore("Append SaveMode doesn't destroy existing data") {
    val expectedCommands =
      Seq("CREATE TABLE IF NOT EXISTS \"PUBLIC\".\"test_table\" .*".r,
        "COPY \"PUBLIC\".\"test_table\" .*".r)

    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped(defaultParams("dbtable")).toString -> null))

    val source = new DefaultSource((_, _) => mockS3Client)
    source.createRelation(testSqlContext, SaveMode.Append, defaultParams, expectedDataDF)

    // This test is "appending" to an empty table, so we expect all our test data to be
    // the only content in the returned data frame.
    // The data should have been written to a random subdirectory of `tempdir`. Since we clear
    // `tempdir` between every unit test, there should only be one directory here.
    assert(s3FileSystem.listStatus(new Path(s3TempDir)).length === 1)
    val dirWithAvroFiles = s3FileSystem.listStatus(new Path(s3TempDir)).head.getPath.toUri.toString
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(dirWithAvroFiles)
    checkAnswer(written, TestUtils.expectedDataWithConvertedTimesAndDates)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  ignore("include_column_list=true adds the schema columns to the COPY query") {
    val expectedCommands = Seq(
      "CREATE TABLE IF NOT EXISTS \"PUBLIC\".\"test_table\" .*".r,

      ("COPY \"PUBLIC\".\"test_table\" \\(\"testbyte\",\"testbool\",\"testdate\"," +
        "\"testdouble\",\"testfloat\",\"testint\",\"testlong\",\"testshort\",\"teststring\"," +
        "\"testtimestamp\"\\) FROM .*").r
    )

    val params = defaultParams ++ Map("include_column_list" -> "true")

    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped(defaultParams("dbtable")).toString -> TestUtils.testSchema))

    val source = new DefaultSource((_, _) => mockS3Client)
    source.createRelation(testSqlContext, SaveMode.Append, params, expectedDataDF)

    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  ignore("include_column_list=false (default) does not add the schema columns to the COPY query") {
    val expectedCommands = Seq(
      "CREATE TABLE IF NOT EXISTS \"PUBLIC\".\"test_table\" .*".r,

      "COPY \"PUBLIC\".\"test_table\" FROM .*".r
    )

    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped(defaultParams("dbtable")).toString -> TestUtils.testSchema))

    val source = new DefaultSource((_, _) => mockS3Client)
    source.createRelation(testSqlContext, SaveMode.Append, defaultParams, expectedDataDF)

    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  test("configuring maxlength on string columns") {
    val longStrMetadata = new MetadataBuilder().putLong("maxlength", 512).build()
    val shortStrMetadata = new MetadataBuilder().putLong("maxlength", 10).build()
    val schema = StructType(
      StructField("long_str", StringType, metadata = longStrMetadata) ::
        StructField("short_str", StringType, metadata = shortStrMetadata) ::
        StructField("default_str", StringType) ::
        Nil)
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val createTableCommand =
      DefaultRedshiftWriter.createTableSql(df, MergedParameters.apply(defaultParams)).trim
    val expectedCreateTableCommand =
      """CREATE TABLE IF NOT EXISTS "PUBLIC"."test_table" ("long_str" VARCHAR(512),""" +
        """ "short_str" VARCHAR(10), "default_str" VARCHAR(MAX))"""
    assert(createTableCommand === expectedCreateTableCommand)
  }

  test("configuring encoding on columns") {
    val lzoMetadata = new MetadataBuilder().putString("encoding", "LZO").build()
    val runlengthMetadata = new MetadataBuilder().putString("encoding", "RUNLENGTH").build()
    val schema = StructType(
      StructField("lzo_str", StringType, metadata = lzoMetadata) ::
        StructField("runlength_str", StringType, metadata = runlengthMetadata) ::
        StructField("default_str", StringType) ::
        Nil)
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val createTableCommand =
      DefaultRedshiftWriter.createTableSql(df, MergedParameters.apply(defaultParams)).trim
    val expectedCreateTableCommand =
      """CREATE TABLE IF NOT EXISTS "PUBLIC"."test_table" ("lzo_str" VARCHAR(MAX)  ENCODE LZO,""" +
        """ "runlength_str" VARCHAR(MAX)  ENCODE RUNLENGTH, "default_str" VARCHAR(MAX))"""
    assert(createTableCommand === expectedCreateTableCommand)
  }

  test("configuring descriptions on columns") {
    val descriptionMetadata1 = new MetadataBuilder().putString("description", "Test1").build()
    val descriptionMetadata2 = new MetadataBuilder().putString("description", "Test'2").build()
    val schema = StructType(
      StructField("first_str", StringType, metadata = descriptionMetadata1) ::
        StructField("second_str", StringType, metadata = descriptionMetadata2) ::
        StructField("default_str", StringType) ::
        Nil)
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val commentCommands =
      DefaultRedshiftWriter.commentActions(Some("Test"), schema)

    val expectedCommentCommands = List(
      "COMMENT ON TABLE %s IS 'Test'",
      "COMMENT ON COLUMN %s.\"first_str\" IS 'Test1'",
      "COMMENT ON COLUMN %s.\"second_str\" IS 'Test''2'")
    assert(commentCommands === expectedCommentCommands)
  }

  test("configuring redshift_type on columns") {
    val bpcharMetadata = new MetadataBuilder().putString("redshift_type", "BPCHAR(2)").build()
    val nvarcharMetadata = new MetadataBuilder().putString("redshift_type", "NVARCHAR(123)").build()

    val schema = StructType(
      StructField("bpchar_str", StringType, metadata = bpcharMetadata) ::
        StructField("bpchar_str", StringType, metadata = nvarcharMetadata) ::
        StructField("default_str", StringType) ::
        Nil)

    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val createTableCommand =
      DefaultRedshiftWriter.createTableSql(df, MergedParameters.apply(defaultParams)).trim
    val expectedCreateTableCommand =
      """CREATE TABLE IF NOT EXISTS "PUBLIC"."test_table" ("bpchar_str" BPCHAR(2),""" +
        """ "bpchar_str" NVARCHAR(123), "default_str" VARCHAR(MAX))"""
    assert(createTableCommand === expectedCreateTableCommand)
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped(defaultParams("dbtable")).toString -> null))
    val errIfExistsSource = new DefaultSource((_, _) => mockS3Client)
    intercept[Exception] {
      errIfExistsSource.createRelation(
        testSqlContext, SaveMode.ErrorIfExists, defaultParams, expectedDataDF)
    }
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  ignore("DataAPI Refactoring TODO - Do nothing when table exists if SaveMode = Ignore") {
    val mockRedshift = new MockRedshift(
      defaultParams("url"),
      Map(TableName.parseFromEscaped(defaultParams("dbtable")).toString -> null))
    val ignoreSource = new DefaultSource((_, _) => mockS3Client)
    ignoreSource.createRelation(testSqlContext, SaveMode.Ignore, defaultParams, expectedDataDF)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  test("Cannot save when 'query' parameter is specified instead of 'dbtable'") {
    val invalidParams = Map(
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
      "tempdir" -> s3TempDir,
      "query" -> "select * from test_table",
      "forward_spark_s3_credentials" -> "true")

    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.write.format("io.github.spark_redshift_community.spark.redshift")
        .options(invalidParams)
        .save()
    }
    assert(e1.getMessage.contains("dbtable"))
  }

  test("Public Scala API rejects invalid parameter maps") {
    val invalidParams = Map("dbtable" -> "foo") // missing tempdir and url

    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.write.format("io.github.spark_redshift_community.spark.redshift")
        .options(invalidParams)
        .save()
    }
    assert(e1.getMessage.contains("tempdir"))

    val e2 = intercept[IllegalArgumentException] {
      expectedDataDF.write.format("io.github.spark_redshift_community.spark.redshift")
        .options(invalidParams)
        .save()
    }
    assert(e2.getMessage.contains("tempdir"))
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }

  test("Saves throw error message if S3 Block FileSystem would be used") {
    val params = defaultParams + ("tempdir" -> defaultParams("tempdir").replace("s3a", "s3"))
    val e = intercept[UnsupportedFileSystemException] {
      expectedDataDF.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .mode("append")
        .options(params)
        .save()
    }
    assert(e.getMessage.contains("No FileSystem for scheme"))
  }

  test("Loads throw error message if S3 Block FileSystem would be used") {
    val params = defaultParams + ("tempdir" -> defaultParams("tempdir").replace("s3a", "s3"))
    val e = intercept[UnsupportedFileSystemException] {
      testSqlContext.read.format("io.github.spark_redshift_community.spark.redshift")
        .options(params)
        .load()
    }
    assert(e.getMessage.contains("No FileSystem for scheme"))
  }

  ignore("DataAPI Refactoring TODO - Redshift reads include region when tempdir_region set") {
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" FROM \"PUBLIC\".\"test_table\" '\\) " +
        "TO '.*' " +
        "WITH CREDENTIALS 'aws_iam_role=fake_role_arn' " +
        "ESCAPE MANIFEST NULL AS '@NULL@'  REGION AS 'us-west-1'").r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithRegion = defaultParams + ("tempdir_region" -> "us-west-1")
    val relation = source.createRelation(
      testSqlContext, paramsWithRegion, TestUtils.testSchema)
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - Redshift reads does not include region when tempdir_region is not set") {
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" FROM \"PUBLIC\".\"test_table\" '\\) " +
        "TO '.*' " +
        "WITH CREDENTIALS 'aws_iam_role=fake_role_arn' " +
        "ESCAPE MANIFEST NULL AS '@NULL@'  REGION AS").r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource((_, _) => mockS3Client)
    val relation = source.createRelation(
      testSqlContext, defaultParams, TestUtils.testSchema)
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    val e = intercept[TestFailedException] {
      mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupPattern, expectedQuery))
    }
    assert(e.getMessage.contains("Actual and expected JDBC queries did not match"))
  }

  ignore("DataAPI Refactoring TODO - Redshift reads include user provided query group label") {
    val userLabel = "expected"
    val queryGroupExpected = s"""set query_group to .*"lbl":"${userLabel}".*"""
    val expectedQuery = (
      "UNLOAD .*").r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithRegion = defaultParams + ("tempdir_region" -> "us-west-1",
      PARAM_USER_QUERY_GROUP_LABEL -> userLabel)
    val relation = source.createRelation(
      testSqlContext, paramsWithRegion, TestUtils.testSchema)
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupExpected.r, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - Redshift reads include spark configuration trace id in query group") {
    val traceId = "expected"
    val queryGroupExpected = s"""set query_group to .*"tid":"${traceId}".*"""
    val expectedQuery = "UNLOAD .*".r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithRegion = defaultParams + ("tempdir_region" -> "us-west-1")
    val relation = source.createRelation(
      testSqlContext, paramsWithRegion, TestUtils.testSchema)
    testSqlContext.sql(s"SET ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF}=${traceId}")
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupExpected.r, expectedQuery))
  }

  def getEditableEnv: util.Map[String, String] = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    field.get(System.getenv()).asInstanceOf[util.Map[String, String]]
  }

  def setEnv(key: String, value: String): Unit = {
    getEditableEnv.put(key, value)
  }

  def unsetEnv(key: String): Unit = {
    getEditableEnv.remove(key)
  }

  ignore("DataAPI Refactoring TODO - Redshift reads include environment variable trace id in query group" +
    "when not provided in spark configuration") {
    val traceId = "expected"
    // Ensure environment variable for trace id is set
    setEnv(Utils.CONNECTOR_TRACE_ID_ENV_VAR, traceId)

    val queryGroupExpected = s"""set query_group to .*"tid":"${traceId}".*"""
    val expectedQuery = "UNLOAD .*".r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithRegion = defaultParams + ("tempdir_region" -> "us-west-1")
    val relation = source.createRelation(
      testSqlContext, paramsWithRegion, TestUtils.testSchema)
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupExpected.r, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - Redshift reads include application id as trace id in query group when none is provided") {
    // ensure that no environment variable is set for trace id
    unsetEnv(Utils.CONNECTOR_TRACE_ID_ENV_VAR)
    val traceId = testSqlContext.sparkContext.applicationId
    val queryGroupExpected = s"""set query_group to .*"tid":"${traceId}".*"""
    val expectedQuery = "UNLOAD .*".r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithRegion = defaultParams + ("tempdir_region" -> "us-west-1")
    val relation = source.createRelation(
      testSqlContext, paramsWithRegion, TestUtils.testSchema)
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupExpected.r, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - Redshift reads include application id as trace id in query" +
    " group when spark configuration is invalid") {
    val expectedTraceId = testSqlContext.sparkContext.applicationId
    val queryGroupExpected = s"""set query_group to .*"tid":"${expectedTraceId}".*"""
    val traceId = """invalidTrace"Id"""
    val expectedQuery = "UNLOAD .*".r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithRegion = defaultParams + ("tempdir_region" -> "us-west-1")
    val relation = source.createRelation(
      testSqlContext, paramsWithRegion, TestUtils.testSchema)
    testSqlContext.sql(s"SET ${Utils.CONNECTOR_TRACE_ID_SPARK_CONF}=${traceId}")
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupExpected.r, expectedQuery))
  }

  ignore("DataAPI Refactoring TODO - Redshift reads include application id as trace id in query" +
    " group when environment variable is invalid") {
    val expectedTraceId = testSqlContext.sparkContext.applicationId
    val queryGroupExpected = s"""set query_group to .*"tid":"${expectedTraceId}".*"""
    val traceId = """invalidTrace"Id"""
    // Ensure environment variable for trace id is set
    setEnv(Utils.CONNECTOR_TRACE_ID_ENV_VAR, traceId)

    val expectedQuery = "UNLOAD .*".r
    val mockRedshift =
      new MockRedshift(defaultParams("url"), Map("test_table" -> TestUtils.testSchema))
    val source = new DefaultSource((_, _) => mockS3Client)
    val paramsWithRegion = defaultParams + ("tempdir_region" -> "us-west-1")
    val relation = source.createRelation(
      testSqlContext, paramsWithRegion, TestUtils.testSchema)
    relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq(queryGroupExpected.r, expectedQuery))
  }
}