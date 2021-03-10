package com.haswalk.fe

import org.apache.spark.sql.{SparkSession, functions}
import org.json.JSONObject
import org.scalatest.funsuite.AnyFunSuite

class MainSuite extends AnyFunSuite{

  test("x output") {

    val spark = SparkSession.builder()
      .appName("app")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq((1, 2), (3, 4), (4, 5), (6, 5)).toDF("a", "b")
      .withColumn("id", functions.monotonically_increasing_id())

    val json = new JSONObject(
      s"""
         |{
         |        "nodes": [{
         |            "name": "a",
         |            "id": "31653c697c554f83a78fd25a86f26d93",
         |            "type": "input",
         |            "params": {
         |                "data_type": "continuous",
         |                "field_type": "tinyint(4)",
         |                "field_name": "a"
         |            }
         |        }],
         |        "name": "x",
         |        "id": "6c55c04e08a34824a78235bc8d1907ea",
         |        "type": "output"
         |    }
         |""".stripMargin)

    val node = parse(json)
    node.fit(df)
      .transform(df)
      .show()


  }

  private def parse(json: JSONObject): Node  = {

    json.get("type").toString match {
      case Node.INPUT =>
        new InputNode(json.getString("name"))
      case Node.OUTPUT =>
        val jarr = json.getJSONArray("nodes")
        val nodes = (0 until jarr.length()).map(i => {
          val jobj = jarr.getJSONObject(i)
          parse(jobj)
        }).toArray
        new OutputNode(nodes)
      case Node.OPERATOR =>
        new NopNode
    }

  }

}
