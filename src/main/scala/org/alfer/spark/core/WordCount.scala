package org.alfer.spark.core

import org.alfer.spark.streaming.StreamingLogger
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author feng.wei
  */
object WordCount {


  def main(args: Array[String]) {
    StreamingLogger.setStreamingLogLevels()

    val path = "hdfs://work82:8020/user/root/README"
    val jobName = "WordCount"
    val sparkConf = new SparkConf()
      .setAppName(jobName)
      //      .set("spark.driver.port", "9400")
      //      .set("spark.port.maxRetries", "100")
      //      .set("spark.ui.port", "4040")
      .set("spark.driver.host", "localhost")
      .setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    // 可以读入多个文件，通过正则进行匹配
    val data = sc.textFile(path)

    val wc = data.flatMap(l => l.trim.split(" "))
      .map(w => (w, 1))

    val wCount = wc.reduceByKey(_ + _)
    //		val wCount = wc.reduceByKey((a, b) => a + b)
    wCount.foreach(print(_))

  }
}
