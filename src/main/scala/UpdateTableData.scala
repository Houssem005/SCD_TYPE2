import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, lit, max, min, trim, when}

import java.time.LocalDate

object UpdateTableData {
  def updateTable(Updates: DataFrame, History: DataFrame): DataFrame = {
    val currentDate = LocalDate.now
    val column_names = Seq("id", "firstname","lastname","address", "moved_in", "moved_out", "current")
    val newUpdates = Updates.withColumn("umoved_out",lit(currentDate)).orderBy(col("uid"),col("umoved_in"))
    val UpdatesAndHistory = History.join(newUpdates,History.col("id")=== newUpdates.col("uid"),"fullouter")
    val UpdatesAndHistoryData = UpdatesAndHistory
      .withColumn(
        "action",
        when(col("uaddress") =!= col("address"), lit("UPSERT"))
          .when(col("id").isNull, lit("INSERT"))
          .otherwise(lit("NOACTION"))
      )
    //for records to be inserted only
    val InsertedData = UpdatesAndHistoryData.filter(
      col("action") === "INSERT")
      .select(
        col("uid").alias("id"),
        col("ufirstname").alias("firstname"),
        col("ulastname").alias("lastname"),
        col("uaddress").alias("address"),
        col("umoved_in").alias("moved_in"),
        col("umoved_out").alias("moved_out"),
        lit(true).alias("current")
      )
    //for records that need no action
    val NoActionData = UpdatesAndHistoryData.filter(col("action") === "NOACTION")
      .withColumn("moved_in", when(
        col("moved_in") > col("umoved_in"), col("umoved_in")
      ).otherwise(col("moved_in")))
      .select(
        column_names.map(col): _*)
    //for record that needs to be expired and then inserted
    //inserting
    val InsertedHistory = UpdatesAndHistoryData.filter(
      col("action") === "UPSERT"
    ).select(
      col("uid").alias("id"),
      col("ufirstname").alias("firstname"),
      col("ulastname").alias("lastname"),
      col("uaddress").alias("address"),
      col("umoved_in").alias("moved_in"),
      col("umoved_out").alias("moved_out"),
      lit(true).alias("current"))
    //updating
    val UpdatedRecordHistory = UpdatesAndHistoryData.filter(
      col("action") === "UPSERT")
      .withColumn("moved_out", when(
        col("current") === true, col("umoved_in")
      ).otherwise(col("moved_out")))
      .withColumn("current", lit(false))
      .select(column_names.map(col): _*)

    val minMovedOutUpdated = UpdatedRecordHistory
      .groupBy("id", "firstname", "lastname", "address", "moved_in")
      .agg(min("moved_out").as("min_moved_out"))

    val UpdatedRecordHistoryData = UpdatedRecordHistory.join(
      minMovedOutUpdated, Seq("id", "firstname", "lastname", "address", "moved_in"))
      .where(
        col("moved_out") === col("min_moved_out"))
      .drop("min_moved_out")

    val UpdatedHistory = NoActionData.union(InsertedData).union(InsertedHistory).union(UpdatedRecordHistoryData)
      .orderBy(col("id"),col("moved_in"))
    val Ordered_Moved_IN = UpdatedHistory.alias("h1").join(
      UpdatedHistory.alias("h2"),
      col("h1.id") === col("h2.id") && col("h2.moved_in") > col("h1.moved_in")
    ).groupBy("h1.id", "h1.moved_in").agg(
      min("h2.moved_in").alias("next_moved_in")
    )
    val UpdatedHistoryData = UpdatedHistory.join(Ordered_Moved_IN, Seq("id", "moved_in"), "left")
      .withColumn(
        "moved_out",
        when(
          col("next_moved_in").isNotNull,
          col("next_moved_in")
        ).otherwise(currentDate)
      ).drop("next_moved_in")
      .select(column_names.map(col): _*)
      .orderBy(col("id"), col("moved_in")).distinct()
    val maxMovedOut = UpdatedHistoryData.groupBy("id").agg(max("moved_in").as("max_moved_in"))
    val UpdatedHistoryRecord = UpdatedHistoryData.join(maxMovedOut, Seq("id"))
      .withColumn("is_latest", col("moved_in") === col("max_moved_in"))
      .withColumn("current", when(col("is_latest") === false, false).otherwise(true))
      .drop("max_moved_in")
      .drop("is_latest")
    UpdatedHistoryRecord.orderBy(col("id"),col("moved_in"))
  }
}
