import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, lit, max, trim, when}

import java.time.LocalDate

object UpdateTableData {
  def updateTable(Updates: DataFrame, History: DataFrame): DataFrame = {
    val currentDate = LocalDate.now
    val column_names = Seq("id", "firstname","lastname","address", "moved_in", "moved_out", "current")
    val newUpdates = Updates.withColumn("umoved_out",lit(currentDate))
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
    val NoActionData = UpdatesAndHistoryData.filter(
      col("action") === "NOACTION"
    ).select(column_names.map(col): _*)
    //for record that needs to be expired and then inserted
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
    val maxMovedOut = InsertedHistory.groupBy("id").agg(functions.max("moved_in").as("max_moved_in"))
    val InsertedHistoryData = InsertedHistory.join(maxMovedOut, Seq("id"))
      .withColumn("is_latest", col("moved_in") === col("max_moved_in"))
      .withColumn("current", when(col("is_latest") === false, false).otherwise(true))
      .drop("max_moved_in")
      .drop("is_latest")
    val InsertedHistoryRecord = InsertedHistoryData.filter(col("current") === true)
    val UpdatedRecordHistory = UpdatesAndHistoryData.filter(
      col("action") === "UPSERT")
      .withColumn("moved_out", when(
        col("current") === true, col("umoved_in")
      ).otherwise(col("moved_out")))
      .withColumn("current", lit(false))
      .select(column_names.map(col): _*)
    val maxMovedOutUpdated = UpdatedRecordHistory.groupBy("id", "firstname", "lastname", "address", "moved_in")
      .agg(max("moved_out").as("max_moved_out"))
    val UpdatedRecordHistoryData = UpdatedRecordHistory.join(maxMovedOutUpdated, Seq("id", "firstname", "lastname", "address", "moved_in"))
      .where(col("moved_out") === col("max_moved_out"))
      .drop("max_moved_out")
    val UpdatedHistory = NoActionData.union(InsertedData).union(InsertedHistoryRecord).union(UpdatedRecordHistoryData).orderBy(col("id"))
    UpdatedHistory
  }
}
