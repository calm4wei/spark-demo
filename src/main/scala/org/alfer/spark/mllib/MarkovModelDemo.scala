package org.alfer.spark.mllib

/**
  * Created by feng.wei at 2018/12/24
  * 马尔可夫邮件预测营销
  */
object MarkovModelDemo {

	/**
	  * 原始数据格式:customerID,transactionID,purchaseDate,amount
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		// 读取原始数据

		// 把customerID作为key(map), <customerID, Tuple2<purchaseDate, amount>>

		// 按照customerID分组(groupByKey), <customerID, Iterator<purchaseDate, amount>>

		// 创建马尔可夫状态序列: 1 对每个customerID的交易按照交易日期排序; 2 将有序列表(按日期)转换为一个状态序列
		// <customerID, List<String>>

		// 生成马尔可夫状态转移矩阵<Tuple2<fromState, toState>, 1> 组合/规约(fromState, toState)的频度
		// =>(reduceByKey) <Tuple2<fromState, toState>, frequency-count> 
		// => Tuple2<fromState, toState>"<tab>"<frequency-count>

		// 生成马尔可夫概率模型

	}

}
