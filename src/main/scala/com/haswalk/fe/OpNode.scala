package com.haswalk.fe

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}

/**
 * 算子节点，抽象了所有特征变换函数
 *
 * @param nodes 上游节点数组
 * */
abstract class OpNode(val nodes: Array[Node]) extends Node{

  protected def assemble(dataset:Dataset[Row], inputs: Array[String], output: String): DataFrame =
    new VectorAssembler()
      .setInputCols(inputs)
      .setOutputCol(output)
      .transform(dataset)


  protected def disassemble(dataset: Dataset[Row], vecColName: String, outputCols: String*): Dataset[Row] = {

    val df = dataset
      .withColumn(vecColName, functions.udf((v: Vector) => {
        v.toArray
      }).apply(functions.col(vecColName)))

    val columns = outputCols.zipWithIndex.map {
      case (col, idx) => functions.col(vecColName).getItem(idx).as(col)
    }

    val cols = Array(functions.col("id")) ++ columns

    df.select(cols:_*)

  }

  protected def getNamesWithoutId(df:Dataset[Row]): Array[String] =
    df.schema.fields.filter(f => !f.name.equals("id"))
      .map(_.name)

}

object OpNode {



}
