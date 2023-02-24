import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, col, explode, lead, lit, max, min, when}

import java.time.LocalDate

object Utils {
  val currentDate = LocalDate.now
  val column_names = Seq("id", "firstname","lastname","address", "moved_in", "moved_out", "current")

  def getJoinedHistoryAndUpdate(Updates: DataFrame, History: DataFrame): DataFrame={
    val UpdatesNew = Updates.withColumn("umoved_out",lit(currentDate)).orderBy("uid","umoved_in")
    val JoinedUpdatesAndHistory = History.join(UpdatesNew,History.col("id")===UpdatesNew.col("uid"),"fullouter")
    val updatesAndHistoryData = JoinedUpdatesAndHistory
      .withColumn("action",
        when(col("uaddress") =!= col("address"),lit("UPSERT"))
          .when(col("id").isNull,lit("INSERT"))
          .otherwise(lit("NOACTION"))
      )
    updatesAndHistoryData
  }
  def insertNewPeopleHistory(updatesAndHistoryData: DataFrame): DataFrame ={
    val InsertedRecords = updatesAndHistoryData.filter(col("action")==="INSERT")
      .select(
        col("uid").alias("id"),
        col("ufirstname").alias("firstname"),
        col("ulastname").alias("lastname"),
        col("uaddress").alias("address"),
        col("umoved_in").alias("moved_in"),
        col("umoved_out").alias("moved_out"),
        lit(true).alias("current")
      )
    InsertedRecords
  }
  def extractNoActionData(updatesAndHistoryData: DataFrame): DataFrame ={
    val NoActionData =updatesAndHistoryData.filter(col("action")==="NOACTION")
      .withColumn("moved_in",
        when(col("moved_in")>col("umoved_in"),col("umoved_in"))
      .otherwise(col("moved_in")))
      .select(column_names.map(col): _*)
    NoActionData
  }
  def insertOldPeopleHistory(UpdatesAndHistory: DataFrame): DataFrame = {
    val OldPeopleHistory = UpdatesAndHistory.filter(
      col("action")==="UPSERT"
    ).select(
      col("uid").alias("id"),
      col("ufirstname").alias("firstname"),
      col("ulastname").alias("lastname"),
      col("uaddress").alias("address"),
      col("umoved_in").alias("moved_in"),
      col("umoved_out").alias("moved_out"),
      lit(true).alias("current"))
    OldPeopleHistory
  }
  def updateOldPeopleRecord(UpdatesAndHistory: DataFrame): DataFrame = {
    val UpdatedRecordHistory = UpdatesAndHistory.filter(
      col("action") === "UPSERT")
      .withColumn("moved_out", when(
        col("current") === true, col("umoved_in")
      ).otherwise(col("moved_out")))
      .withColumn("current", lit(false))
      .select(column_names.map(col): _*)
    val minMovedOutUpdating = UpdatedRecordHistory.groupBy("id", "firstname", "lastname", "address", "moved_in")
      .agg(min("moved_out").as("min_moved_out"))

    val UpdatedRecordHistoryData = UpdatedRecordHistory.join(minMovedOutUpdating, Seq("id", "firstname", "lastname", "address", "moved_in"))
      .where(col("moved_out") === col("min_moved_out"))
      .drop("min_moved_out")
    UpdatedRecordHistoryData
  }
  def updateHistory(NoActionData: DataFrame,InsertedData:DataFrame,InsertedHistory:DataFrame,UpdatedRecordHistoryData:DataFrame): DataFrame = {
    val UpdatedHistory = NoActionData.union(InsertedData).union(InsertedHistory).union(UpdatedRecordHistoryData)
      .orderBy(col("id"), col("moved_in")).distinct()
    UpdatedHistory
  }
  def orderingHistoryDates(UpdatedHistory:DataFrame): DataFrame = {
    val sortedDates = UpdatedHistory
      .withColumn("dates", array(col("moved_in"), col("moved_out")))
      .withColumn("all_dates", explode(col("dates")))
      .select(col("id"), col("all_dates"))
      .orderBy(col("id"), col("all_dates"))
      .distinct()

    val window = Window.partitionBy("id").orderBy("all_dates")
    val SortedRecords = sortedDates.withColumn("moved_out", lead("all_dates", 1).over(window))
      .select("id", "all_dates", "moved_out")
      .withColumnRenamed("all_dates", "moved_in")
    val SortedRecordsData = SortedRecords.select(
      col("id").alias("sid"),
      col("moved_in").alias("smoved_in"),
      col("moved_out").alias("smoved_out"))
    val UpdatedHistoryInfo = UpdatedHistory.join(SortedRecordsData, SortedRecordsData.col("sid") === UpdatedHistory.col("id")
      && SortedRecordsData.col("smoved_in") === UpdatedHistory.col("moved_in"), "left")
    val OrderedUpdatedHistory = UpdatedHistoryInfo
      .withColumn("moved_out",
        col("smoved_out"))
      .drop("sid", "smoved_in", "smoved_out")
    OrderedUpdatedHistory
  }
  def setOnlyLatestRecordTrue(OrderedUpdatedHistory:DataFrame): DataFrame = {
    val MaxMovedIN = OrderedUpdatedHistory.groupBy("id").agg(max("moved_in").alias("max_moved_in"))
    val history = OrderedUpdatedHistory.join(MaxMovedIN,Seq("id"))
      .withColumn("is_latest",col("moved_in")===col("max_moved_in"))
      .withColumn("current",
        when(col("is_latest") === false, false)
          .when(col("moved_out") =!= currentDate, false).otherwise(true))
      .drop("max_moved_in")
      .drop("is_latest")
    history.orderBy(col("id"),col("moved_in"))
  }
}
