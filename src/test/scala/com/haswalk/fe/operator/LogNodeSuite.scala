package com.haswalk.fe.operator

import com.haswalk.fe.InputNode
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite


class LogNodeSuite extends AnyFunSuite{

  test("fit") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("app")
      .getOrCreate()

    import spark.implicits._

    val df = Seq((1, 2, 3), (3, 4, 5), (4, 3, 2)).toDF("a", "b", "c")
      .withColumn("id", functions.monotonically_increasing_id())

    val transformed = new LogNode(10, Array(
      new InputNode("a"),
      new InputNode("b"),
      new InputNode("c")
    ))
      .fit(df)
      .transform(df)

    assert(transformed != null)
    transformed.show()

  }

}
