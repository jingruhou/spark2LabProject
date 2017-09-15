package com.xunfang.spark2.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountSample {
  def main(args: Array[String]) {

    /**
      * 创建批次大小为1秒的StreamingContext上下文
      */
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    /**
      * 在目标ip：port上创建一个套接字流并对其
      * (\n分隔文本的输入流中的单词《例如，由'nc生成》)进行计数,
      *
      * 注意，只有在本地运行时，存储级别不会重复。
      * 在分布式场景中对容错进行复制。
      */
    val lines = ssc.socketTextStream("127.0.0.1", 8341, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    /**
      * 启动执行流
      */
    ssc.start()
    ssc.awaitTermination()
  }
}
