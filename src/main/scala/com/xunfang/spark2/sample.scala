package com.xunfang.spark2

import org.apache.spark.{SparkConf, SparkContext}

object sample {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化环境配置
      */
    val conf = new SparkConf().setAppName("sample").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /**
      * 加载数据
      */
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    /**
      * 打印数据
      */
    distData.foreach(println)

    /**
      * 2017-09-29 新环境测试
      */
    val data1 = Array(9,8,7,6,5,4)
    val distData1 = sc.parallelize(data1)

    distData1.foreach(println)
  }
}
