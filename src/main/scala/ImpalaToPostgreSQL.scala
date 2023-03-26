import org.apache.spark.sql.SparkSession

import java.util.Properties


object ImpalaToPostgreSQL {


  val spark = SparkSession.builder().appName("ImpalaToSpark").getOrCreate()

  // Define the Impala table name and JDBC connection parameters
  val impalaTableName = "systakipno_offset_new"
  val impalaJdbcUrl = "jdbc:impala://opn114.saglik.lokal:21050/uss_meta;AuthMech=3"
  val props = new Properties()

  // set new properties
  props.setProperty("UID", "user")
  props.setProperty("PWD", "pass")
  props.setProperty("dbtable", "systakipno_offset_new")
  // Define the number of partitions and the offset column
  val numPartitions = 4
  val offsetColumn = "offset"

  // Define the batch size
  val batchSize = 1000

  // Define the columns to select from the Impala table
  val selectColumns = Seq("id", "name", "age")

  // Define the lower and upper bounds for the offset column
  val lowerBound = spark.read.jdbc(impalaJdbcUrl, impalaTableName,props).selectExpr(s"min($offsetColumn)").collect()(0)(0).asInstanceOf[Long]
  val upperBound = spark.read.jdbc(impalaJdbcUrl, impalaTableName,props).selectExpr(s"max($offsetColumn)").collect()(0)(0).asInstanceOf[Long]

  // Define the partition column and partitioning strategy
  val partitionColumn = offsetColumn
  val partitioning = "range"

  // Read data from Impala table into a Spark DataFrame with the specified number of partitions, offset column, and batch size
  val impalaDataFrame = spark.read.format("jdbc")
    .option("url", impalaJdbcUrl)
    .option("dbtable", impalaTableName)
    .option("numPartitions", numPartitions)
    .option("partitionColumn", partitionColumn)
    .option("lowerBound", lowerBound)
    .option("upperBound", upperBound)
    .option("fetchSize", batchSize)
    .option("partitionStrategy", partitioning)
    .option("driver", "com.cloudera.impala.jdbc41.Driver")
    .load()
    .selectExpr(selectColumns: _*)


}
