package com.xunfang.spark2.StructuredStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StructuredStreamingSample {
  def main(args: Array[String]): Unit = {
    /**
      * 添加日志打印级别
      */
    Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * 初始化环境配置
      */
    val spark = SparkSession
      .builder()
      .appName("StructuredStreamingSample")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", "8341")
      .load()

    /**
      * 切分行数据
      */
    val words = lines.as[String].flatMap(_.split(" "))

    /**
      * Generate running word count
      * 生成运行计数(词频统计)
      */
    val wordCounts = words.groupBy("value").count()

    /**
      * 开始运行 将运行计数(词频统计)打印到控制台 的查询
      */
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
