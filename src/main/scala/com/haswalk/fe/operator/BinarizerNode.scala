package com.haswalk.fe.operator

import com.haswalk.fe.{Node, OpNode}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{Dataset, Row}

/**
 * 二值化算子节点
 *
 * @param thresh 二值化阈值
 * */
class BinarizerNode(thresh: Double, nodes: Array[Node]) extends OpNode(nodes){

  private val binarizer: Binarizer = new Binarizer().setThreshold(thresh)

  override def fit(dataset: Dataset[Row]): Node = {
    nodes.foreach(_.fit(dataset))
    this
  }

  override def transform(dataset: Dataset[Row]): Dataset[Row] = {

    val dfArray = nodes.map(_.transform(dataset))
    val df = mergeDF(dfArray)

    val names = getNamesWithoutId(df)

    val vectorDF = assemble(df, names, "vector")

    val transformed = binarizer.setInputCol("vector")
      .setOutputCol("output")
      .transform(vectorDF)

    disassemble(transformed, "output", names:_*)

  }

}
