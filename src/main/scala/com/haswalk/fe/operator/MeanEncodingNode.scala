package com.haswalk.fe.operator

import com.haswalk.fe.{Node, OpNode}
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, functions}

class MeanEncodingNode(node: Node) extends OpNode(Array(node)){

  private var encoding: Map[String, Double] = _

  override def fit(dataset: Dataset[Row]): Node = {

    val df = node.fit(dataset)
      .transform(dataset)

    val name = getNamesWithoutId(df)(0)

    val targetDF = dataset.select("id","target")

    val df2 = df.join(targetDF, "id")

    this.encoding = df2.groupBy(name)
      .agg(functions.mean("target").as("mean"))
      .collect()
      .map(row => {
        val k = row.getAs[String](0)
        val v = row.getAs[Double](1)
        (k, v)
      })
      .toMap

    this

  }

  override def transform(dataset: Dataset[Row]): Dataset[Row] = {

    val df = node.transform(dataset)
    val name = getNamesWithoutId(df)(0)

    val encoding = this.encoding
    df.map(new MapFunction[Row, Row]() {
        override def call(row: Row): Row = {
          val k = row.getAs[String](name)
          val id = row.getAs[Long]("id")
          val d = encoding.getOrElse(k, 0)
          Row(d, id)
        }
      }, RowEncoder(StructType.fromDDL(s"$name Double, id Long")))

  }
}
