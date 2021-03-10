package com.haswalk.fe.operator

import com.haswalk.fe.InputNode
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite

class StandardlizeNodeSuite extends AnyFunSuite{

  test("fit") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("app")
      .getOrCreate()

    import spark.implicits._

    val df = Seq((1, 2, 3), (3, 4, 5), (5, 6, 7), (3, 2, 1)).toDF("a", "b", "c")
      .withColumn("id", functions.monotonically_increasing_id())

    val result = new StandardlizeNode(Array(
      new InputNode("a"),
      new InputNode("b"),
      new InputNode("c")
    ))
      .fit(df)
      .transform(df)

    assert(result != null)
    result.show()

  }

}
