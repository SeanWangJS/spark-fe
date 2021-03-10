package com.haswalk.fe

import org.apache.spark.sql.{Dataset, Row}

class NopNode extends Node{

  override def fit(dataset: Dataset[Row]): Node = this
  override def transform(dataset: Dataset[Row]): Dataset[Row] = dataset

}
