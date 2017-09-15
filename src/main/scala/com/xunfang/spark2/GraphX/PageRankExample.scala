package com.xunfang.spark2.GraphX

import org.apache.log4j.{Level, Logger}

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object PageRankExample {
  def main(args: Array[String]): Unit = {
    /**
      * 设置日志输出级别
      */
    Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * 创建SparkSession
      */
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext

    /**
      * 加载边作为图
      */
    val graph = GraphLoader.edgeListFile(sc, "Resource/data/graphx/followers.txt")

    /**
      * 运行PageRank算法
      */
    val ranks = graph.pageRank(0.0001).vertices

    /**
      * Join the ranks with the usernames
      */
    val users = sc.textFile("Resource/data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    /**
      * 打印结果
      */
    println(ranksByUsername.collect().mkString("\n"))

    /**
      * 停止SparkSession
      */
    spark.stop()
  }
}
