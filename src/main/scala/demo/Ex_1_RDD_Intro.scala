package demo

import org.apache.spark.sql.SparkSession

object Ex_1_RDD_Intro {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "d:\\hadoop_home_dir");

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("RDD_Intro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ds = spark.range(1000)
    println(ds.count)
    println(ds.rdd.count)

  }

}
