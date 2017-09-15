package com.xunfang.spark2.MLlib

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.{SparkConf, SparkContext}

object SVMWithSGDsample {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化环境配置
      */
    val conf = new SparkConf().setAppName("SVMWithSGDsample").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /**
      * 加载LIBSVM格式的训练数据
      */
    val data = MLUtils.loadLibSVMFile(sc, "Resource/data/mllib/sample_libsvm_data.txt")

    /**
      * 切分数据集(60%训练集 40%测试集)
      */
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    /**
      * 运行训练算法，构建模型
      */
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    /**
      * 清除默认阈值
      */
    model.clearThreshold()

    /**
      * 在测试集上计算原始分数
      */
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    /**
      * 计算得到评估指标
      */
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    /**
      * 保存、加载模型
      */
    model.save(sc, "Resource/model/scalaSVMWithSGDModel")
    val sameModel = SVMModel.load(sc, "Resource/model/scalaSVMWithSGDModel")

    /**
      * 关闭SparkContext对象
      */
    sc.stop()
  }
}
