package com.haswalk.fe.operator

import com.haswalk.fe.{Node, OpNode}
import org.apache.spark.ml.feature.Logarithmer
import org.apache.spark.sql.{Dataset, Row}

class LogNode(base: Double, nodes: Array[Node]) extends OpNode(nodes){

  override def fit(dataset: Dataset[Row]): Node = {
    nodes.foreach(_.fit(dataset))
    this
  }

  override def transform(dataset: Dataset[Row]): Dataset[Row] = {

    val dfArray = nodes.map(_.transform(dataset))
    val df = mergeDF(dfArray)

    val names = getNamesWithoutId(df)
    val vectorDF = assemble(df, names, "vector")

    val transformed = new Logarithmer()
      .setInputCol("vector")
      .setOutputCol("output")
      .setBase(base)
      .transform(vectorDF)

    disassemble(transformed, "output", names:_*)

  }

}
