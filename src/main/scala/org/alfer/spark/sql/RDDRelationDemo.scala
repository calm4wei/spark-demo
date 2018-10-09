package org.alfer.spark.sql

import com.mongodb.spark.MongoSpark
import org.alfer.spark.streaming.StreamingLogger
import org.apache.spark.sql.{SparkSession}

/**
  * Created by alfer on 7/22/17.
  */
object RDDRelationDemo {

  def main(args: Array[String]): Unit = {
    StreamingLogger.setStreamingLogLevels()

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .config("spark.mongodb.input.uri", "mongodb://dev60/hyjj.ELPModel")
      .getOrCreate()

    //    val options = Map[String, String](
    //      "host" -> "dev60",
    //      "database" -> "hyjj"eee,
    //      "collection" -> "ELPModel"
    //    )

    val df1 = MongoSpark.load(sparkSession)
    df1.printSchema()

    val df2 = sparkSession.read.format("com.mongodb.spark.sql").load()
    df2.show()
    val ds3 = df2.as("ds").select("ds.creator", "ds.createTime", "ds.entities", "ds.links", "ds.graphState")
    ds3.show()

  }
}
