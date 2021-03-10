package com.haswalk.fe.operator

import com.haswalk.fe.InputNode
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite

class MinMaxNodeSuite extends AnyFunSuite{

  test("fit") {

    val spark = SparkSession.builder()
      .appName("app")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq((1,6), (2, 4), (3, 5), (4, 6)).toDF("a", "b")
      .withColumn("id", functions.monotonically_increasing_id())

    val node = new MinMaxNode(0, 1, Array(
      new InputNode("a"),
      new InputNode("b")
    ))

    val minmax = node.fit(df)
    val transformed = minmax.transform(df)
    assert(transformed != null)
    transformed.show()
  }

}
