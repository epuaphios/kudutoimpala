
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
object Main {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf()
    config.set("spark.sql.shuffle.partitions", "300")
    config.set("maxOffsetsPerTrigger", "10196")
    val spark = SparkSession.builder().config(config).master("local[5]").appName("SparkByExamples").getOrCreate()
    for (a <- 37499998 to 253328282 by 50000) {
      val df = spark.read.format("jdbc").option("url", "jdbc:impala://opn114.saglik.lokal:21050/uss_meta;AuthMech=3").option("UID", "kdsbdadmin").option("PWD", "68E,h9g+TT").option("dbtable", "systakipno_offset").option("numPartitions", 5).option("partitionColumn", "offset").option("lowerBound", a).option("upperBound", a + 50000).option("driver", "com.cloudera.impala.jdbc41.Driver").load()
      val df1 = df.filter(col("offset") > a && col("offset") < a + 50000).where(col("topic") =!= "sysgizhash")
      df1.write.mode("append").format("jdbc").option("batchsize", 100000).option("driver", "org.postgresql.Driver").option("url", "jdbc:postgresql://172.18.237.36:5436/kds_test").option("user", "kds_admin").option("dbtable", "kds.systakipno_offset").option("password", "3A%dEv6!Zy").save()
    }

  }
}