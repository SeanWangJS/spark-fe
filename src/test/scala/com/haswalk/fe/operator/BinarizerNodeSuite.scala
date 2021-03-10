package com.haswalk.fe.operator

import com.haswalk.fe.InputNode
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite

class BinarizerNodeSuite extends AnyFunSuite{

  test("fit") {

    val spark = SparkSession.builder()
      .appName("app")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq((1, 2), (3, 4), (4, 5)).toDF("a", "b")
      .withColumn("id", functions.monotonically_increasing_id())

    val transformed = new BinarizerNode(
      2.5,
      Array(
        new InputNode("a"),
        new InputNode("b")
      )
    )
      .fit(df)
      .transform(df)

    assert(transformed != null)

    transformed.show()
  }

}
