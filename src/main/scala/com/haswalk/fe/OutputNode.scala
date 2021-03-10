package com.haswalk.fe

import org.apache.spark.sql.{Dataset, Row}

/**
 * 输出节点，输出特征提取后的数据
 * @param nodes 上游节点数组
 * */
class OutputNode(val nodes: Array[Node]) extends Node{

  private var fittedNodes: Array[Node] = _

  /**
   * 预处理函数，计算所有算子节点的统计参数
   *
   * @param dataset 数据集
   * */
  override def fit(dataset: Dataset[Row]): Node = {
    fittedNodes = nodes.map(_.fit(dataset))
    this
  }

  /**
   * 数据变换函数
   * */
  override def transform(dataset: Dataset[Row]): Dataset[Row] = {
    val dfArray = fittedNodes.map(_.transform(dataset))
    mergeDF(dfArray)
  }
}
