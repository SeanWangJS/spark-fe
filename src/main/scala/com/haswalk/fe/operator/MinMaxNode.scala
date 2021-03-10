package com.haswalk.fe.operator

import com.haswalk.fe.{Node, OpNode}
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel}
import org.apache.spark.sql.{Dataset, Row}

/**
 * min-max 归一化节点
 * */
class MinMaxNode(min: Double, max: Double, nodes: Array[Node]) extends OpNode(nodes){

  private var model: MinMaxScalerModel = _

  override def fit(dataset: Dataset[Row]): Node = {

    // 对所有子节点数据进行变换，并合并数据框
    val dfArray = nodes.map(_.fit(dataset))
      .map(_.transform(dataset))
    val df = mergeDF(dfArray)

    val names = getNamesWithoutId(df)

    val vectorDF = assemble(df, names, "vector")

    this.model = new MinMaxScaler().setMin(min).setMax(max).setInputCol("vector")
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
