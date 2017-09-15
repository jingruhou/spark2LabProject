package com.xunfang.spark2.SparkSQL

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession


object SqlSample {
  def main(args: Array[String]): Unit = {

    /**
      * 添加日志打印级别
      */
    Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * 初始化Spark SQL环境配置
      */
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[4]")
      .config("spark.some.config.option","some-value")
      .getOrCreate()

    /**
      * For implicit conversions like converting RDDs to DataFrames
      *
      * 用于隐式转换，如将RDD转换为DataFrames
      */
    import spark.implicits._

    val df = spark.read.json("Resource/examples/people.json")
    df.show()
  }
}
