package com.haswalk.fe

import org.apache.spark.sql.{Dataset, Row}

/**
 * 输入节点，其代表结构化数据集的一列
 * @param name 列名称
 * */
class InputNode(name: String) extends Node{

  /**
   * 预处理函数，计算所有算子节点的统计参数
   *
   * @param dataset 数据集
   * */
  override def fit(dataset: Dataset[Row]): Node = {
    this
  }

  /**
   * 数据变换函数
   * */
  override def transform(dataset: Dataset[Row]): Dataset[Row] = dataset.select("id", name)
}
