package org.alfer.spark.mllib

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by feng.wei at 2018/10/9
  */
object MovieRecommendation {

	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf()
			.setAppName("MovieRecommendation")
			.setMaster("local[4]")
		val sc = new SparkContext(sparkConf)
		val data = sc.textFile("hdfs://172.30.6.25:8020/user/root/data/movie")
		// 找出谁曾对这个电影评分
		val moviesRDD = data.map(m => {
			val v = m.split(" ")
			// (movie, user, rate)
			(v(1), (v(0), v(2).toInt))
		})
		println("=== debug1: moviesRDD: (movie, (user, rating)) ===")
		moviesRDD.collect().foreach(f => println(s"debug1 movie:${f._1}, (user, rating): ${f._2}"))
		// 按movie分组
		val moviesGrouped = moviesRDD.groupByKey()
		println("=== debug2: movieGrouped: (movie, (user, rating)) ===")
		moviesGrouped.collect().foreach(f => println(s"debug2 movie:${f._1}, value:${f._2}"))

		// 找出每个电影的评分人数
		val usersRDD = moviesGrouped.flatMap(fm => {
			val movie = fm._1
			val userAndRating = fm._2
			val ratingNumbers = userAndRating.size
			userAndRating.map(m => (m._1, (movie, m._2, ratingNumbers)))
		})
		println("=== debug3 usersRDD: (user, (movie, rating ,ratingNumbers))")
		usersRDD.collect().foreach(f => println(s"debug3 user:${f._1}, value:${f._2}"))

		// 完成自连接，找出每个人同时评价的两部电影
		val joinedRDD = usersRDD.join(usersRDD)
		println("=== debug4: joinedRDD: (user, [(movie, rating, ratingNumbers)]")
		joinedRDD.collect().foreach(f => println(s"debug4 user:${f._1}, value:${f._2}"))

		val fiteredRDD = joinedRDD.filter(f => if (f._2._1._1.compareTo(f._2._2._1) < 0) true else false)
		println("=== debug5: filteredRDD")
		fiteredRDD.collect().foreach(f => println(s"debug5: user:${f._1}, value:${f._2}"))

		// 生成(movie1, movie2)组合
		val moviePairs = fiteredRDD.map(m => {
			val k = (m._2._1._1, m._2._2._1)
			val movie1 = m._2._1
			val movie2 = m._2._2

			val v = (
				movie1._2, // movie1.rating
				movie1._3, // movie1.ratingNumbers
				movie2._2, // movie2.rating
				movie2._3, // movie2.ratingNumbers
				movie1._2 * movie2._2, // ratingProduct
				movie1._2 * movie1._2, // rating1Squared
				movie2._2 * movie2._2 // rating2Squared
			)
			(k, v)
		})
		println("=== debug6: moviePairs")
		moviePairs.collect().foreach(f => println(s"debug6: k:${f._1}, v:${f._2}"))

		// 电影对分组
		val corrRDD = moviePairs.groupByKey()

		// TODO 计算关联度 (皮尔逊关联度，余弦相似度，杰卡德相似度)
		val corr = corrRDD.mapValues(v => v)
	}

}
