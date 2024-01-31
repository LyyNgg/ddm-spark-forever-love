package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // TODO
    //First read data from input files
    val tables = inputs.map(input => readData(input, spark))

    //Retrieve columns values and their names
    //Then map them to (value, [ColumnName])
    //Then group by value and add possible columns for value to [columnName] set
    //Remove duplicate values

    val valueSets = tables.flatMap(df => df.columns.map(col => df.select(col).rdd.map(r => (r(0).toString, Set[String](col)))))
      .reduce(_ union _)
      .reduceByKey(_ ++ _).values.distinct()

    // valueSets.foreach(e => println(e))


    //create inclusionList
    val includeList = valueSets.flatMap(set => set.map(column => (column, set - column)))
    // includeList.foreach(i => println(i))


    //intersect includeList to find out common value, then filter to keep only non-empty ones
    val intersectSet = includeList.reduceByKey(_.intersect(_))
      .filter(_._2.nonEmpty).sortByKey(numPartitions = 1)

    // intersectSet.foreach(i => println("intersectSet"+ i))

    //Iterate over each
    intersectSet.foreach(columnName =>
      println(columnName._1 + " < " + columnName._2.mkString(", "))
    )


  }
}

