package com.haswalk.fe.operator

import com.haswalk.fe.{Node, OpNode}
import org.apache.spark.sql.{Dataset, Row}

/**
 * 等距分桶算子节点
 * */
class EqualWidthBucketNode(buckets: Integer, nodes: Array[Node]) extends OpNode(nodes){

  override def fit(dataset: Dataset[Row]): Node = {

    ???

  }

  override def transform(dataset: Dataset[Row]): Dataset[Row] = ???
}
