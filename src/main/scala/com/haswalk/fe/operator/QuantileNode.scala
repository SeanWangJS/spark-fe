package com.haswalk.fe.operator

import com.haswalk.fe.{Node, OpNode}
import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}
import org.apache.spark.sql.{Dataset, Row, functions}

/**
 * 分位数数据分桶算子
 * @param bucket 分桶数量
 * @param nodes 依赖节点数组
 * */
class QuantileNode(bucket: Int, nodes: Array[Node]) extends OpNode(nodes){

  private var model: Bucketizer = _

  override def fit(dataset: Dataset[Row]): Node = {

    val dfArray = nodes.map(_.fit(dataset))
      .map(_.transform(dataset))

    val df = mergeDF(dfArray)

    val names = getNamesWithoutId(df)

    this.model = new QuantileDiscretizer()
      .setInputCols(names)
      .setOutputCols(names.map(_+"_"))
      .setNumBuckets(bucket)
      .fit(df)

    this
  }
  override def transform(dataset: Dataset[Row]): Dataset[Row] = {

    val dfArray = nodes.map(_.transform(dataset))
    val df = mergeDF(dfArray)

    val names = getNamesWithoutId(df)

    val columns = Array(functions.col("id")) ++ names.map(name => functions.col(name + "_"))

    model.transform(df)
      .select(columns:_*)
      .toDF((Array("id") ++ names):_*)

  }
}
