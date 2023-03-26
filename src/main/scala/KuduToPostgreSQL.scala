import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession

import java.io.FileInputStream
import java.util.Properties

object KuduToPostgreSQL {

  val props = new Properties()
  props.load(new FileInputStream("config.properties"))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KuduToPostgreSQL")
      .getOrCreate()

    // Kudu options
    val kuduMaster = "kudu_master_host:port"
    val kuduTableName = "kudu_table_name"
    val kuduOptions = Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> kuduTableName,
      "kudu.num_partitions" -> "10", // Set the number of partitions to 10
      "kudu.batch_size" -> "1000" // Set the batch size to 10,000 records
    )

    // PostgreSQL options
    val pgUrl = props.getProperty("db.url")
    val pgUser = props.getProperty("db.user")
    val pgPassword = props.getProperty("db.password")
    val pgTable = props.getProperty("db.table")


    // Read data from Kudu
    val kuduDF = spark.read.options(kuduOptions).kudu.repartition(10)

    // Write data to PostgreSQL in batches of 50,000
    kuduDF.write.mode("append").format("jdbc").option("batchsize", 1000).option("driver", "org.postgresql.Driver").option("url", pgUrl).option("user", pgUser).option("dbtable", pgTable).option("password", pgPassword).save()


    spark.stop()
  }
}
