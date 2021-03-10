package com.haswalk.fe

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.{Dataset, Row}

trait Node {

  /**
   * 预处理函数，计算所有算子节点的统计参数
   * @param dataset 数据集
   * */
  def fit(dataset: Dataset[Row]): Node

  /**
   * 数据变换函数
   * */
  def transform(dataset: Dataset[Row]): Dataset[Row]

  def mergeDF(dfArray: Array[Dataset[Row]]): Dataset[Row] = {
    val inc = new AtomicInteger(0)
    dfArray.map(nodeDF => {
      val renames = nodeDF.schema.fields
        .map(f => {
          if (f.name.equals("id")) "id" else s"_c${inc.getAndIncrement()}"
        })
      nodeDF.toDF(renames:_*)
    })
      .reduce((left, right) => left.join(right, "id"))

  }
}

object Node {
  val INPUT = "input"
  val OUTPUT = "output"
  val OPERATOR = "operator"
}
