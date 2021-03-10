package com.haswalk.fe

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class InputNodeSuite extends AnyFunSuite{

  test("input node") {

    val spark = SparkSession.builder()
      .appName("app")
      .master("local[*]")
      .getOrCreate()

  }

}
