package com.haswalk.fe.operator

import com.haswalk.fe.{Node, OpNode}
import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.sql.{Dataset, Row}

/**
 * 数据标准化转换算子节点
 * */
class StandardlizeNode(nodes: Array[Node]) extends OpNode(nodes){

  private var model: StandardScalerModel = _

  override def fit(dataset: Dataset[Row]): Node = {

    val dfArray = nodes.map(_.fit(dataset))
      .map(_.transform(dataset))
    val df = mergeDF(dfArray)

    val names = getNamesWithoutId(df)

    val vectorDF = assemble(df, names, "vector")

    this.model = new StandardScaler().setInputCol("vector")
      .setOutputCol("output")
      .fit(vectorDF)

    this
  }

  override def transform(dataset: Dataset[Row]): Dataset[Row] = {

    val dfArray = nodes.map(_.transform(dataset))
    val df = mergeDF(dfArray)

    val names = getNamesWithoutId(df)

    val vectorDF = assemble(df, names, "vector")

    val transformDF = model.transform(vectorDF)
    disassemble(transformDF, "output", names:_*)

  }
}
