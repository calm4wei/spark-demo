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

		val path = "hdfs://master01:9001/user/alfer/wc.txt"
		val jobName = "WordCount"
		val sparkConf = new SparkConf()
			.setAppName(jobName)
			.setMaster("local[4]")
		val sc = new SparkContext(sparkConf)

		// 可以读入多个文件，通过正则进行匹配
		val data = sc.textFile(path)

		val wc = data.flatMap(l => l.trim.split(" "))
			.map(w => (w, 1))

		val wCount = wc.reduceByKey((a, b) => a + b)
		wCount.foreach(print(_))

	}
}
