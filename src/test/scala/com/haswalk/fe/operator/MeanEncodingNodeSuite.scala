package com.haswalk.fe.operator

import com.haswalk.fe.InputNode
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.funsuite.AnyFunSuite

class MeanEncodingNodeSuite extends AnyFunSuite{

  test("fit") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("app")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(("a", 1), ("b", 2), ("c", 4), ("a", 3), ("c", 6))
      .toDF("x", "target")
      .withColumn("id", functions.monotonically_increasing_id())

    new MeanEncodingNode(
      new InputNode("x")
    )
      .fit(df)
      .transform(df)
      .show()


  }

}
