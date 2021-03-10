package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.param.{DoubleParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, functions}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}

import scala.collection.mutable

/**
 * Logarithm a column of continuous features given a base
 * */
class Logarithmer (override val uid: String) extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("logarithmer"))

  /**
   * Default: 10
   * @group param
   * */
  val base: DoubleParam = new DoubleParam(this, "base", "base used to logarithm continuous features")

  def getBase: Double = $(base)

  def setBase(value: Double): this.type= set(base, value)

  setDefault(base -> 10.0)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val bs = $(base)

    val d = Math.log(bs)

    val logarithmDouble = functions.udf((in: Double) => Math.log(in) / d)
    val logarithmVector = functions.udf((data: Vector) => {

      val indices = mutable.ArrayBuilder.make[Int]
      val values = mutable.ArrayBuilder.make[Double]

      data.foreachActive {
        (index, value) =>
          indices += index
          values += Math.log(value) / d
      }

      Vectors.sparse(data.size, indices.result(), values.result()).compressed

    })

    val metadata = outputSchema($(outputCol)).metadata

    inputType match {
      case DoubleType | IntegerType =>
        dataset.select(functions.col("*"), logarithmDouble(functions.col($(inputCol))).as($(outputCol), metadata))
      case _: VectorUDT =>
        dataset.select(functions.col("*"), logarithmVector(functions.col($(inputCol))).as($(outputCol), metadata))
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {

    val inputType = schema($(inputCol)).dataType
    val outputColName = $(outputCol)

    val outCol = inputType match {
      case DoubleType | IntegerType =>
        BinaryAttribute.defaultAttr.withName(outputColName).toStructField()
      case _: VectorUDT =>
        StructField(outputColName, new VectorUDT)
      case _ =>
        throw new IllegalArgumentException(s"Data type $inputType is not supported.")
    }

    if(schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exist.")
    }

    StructType(schema.fields :+ outCol)
  }
}

object Logarithmer extends DefaultParamsReadable[Logarithmer] {
  override def load(path: String): Logarithmer = super.load(path)
}