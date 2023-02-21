import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, max, min, when}

import java.time.LocalDate

object UpdatingTable {
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
  def InsertNewPeopleHistory(updatesAndHistoryData: DataFrame): DataFrame ={
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
  def ExtractNoActionData(updatesAndHistoryData: DataFrame): DataFrame ={
    val NoActionData =updatesAndHistoryData.filter(col("action")==="NOACTION")
      .withColumn("moved_in",
        when(col("moved_in")>col("umoved_in"),col("umoved_in"))
      .otherwise(col("moved_in")))
      .select(column_names.map(col): _*)
    NoActionData
  }
  def InsertOldPeopleHistory(UpdatesAndHistory: DataFrame): DataFrame = {
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
  def UpdateOldPeopleRecord(UpdatesAndHistory: DataFrame): DataFrame = {
    val UpdatedRecordHistory = UpdatesAndHistory.filter(
      col("action")==="UPSERT"
    ).withColumn("moved_out",when(
      col("current")===true,col("umoved_out")
    ).otherwise(col("moved_out")))
      .withColumn("current",lit(false))
      .select(column_names.map(col):_*)
    val minMovedOut = UpdatedRecordHistory.groupBy(
      "id","firstname","address","lastname","moved_in")
      .agg(min("moved_out").as("min_moved_out"))
    val UpdatedRecordHistoryData = UpdatedRecordHistory.join(minMovedOut,Seq("id","firstname","lastname","address","moved_in"))
      .where(col("moved_out")===col("min_moved_out"))
      .drop("min_moved_out")
    UpdatedRecordHistoryData
  }
  def UpdateHistory(NoActionData: DataFrame,InsertedData:DataFrame,InsertedHistory:DataFrame,UpdatedRecordHistoryData:DataFrame): DataFrame = {
    val UpdatedHistory = NoActionData.union(InsertedData).union(InsertedHistory).union(UpdatedRecordHistoryData)
      .orderBy(col("id"), col("moved_in"))
    UpdatedHistory
  }
  def OrderingHistoryDates(UpdatedHistory:DataFrame): DataFrame = {
    val Ordering_Moved_IN = UpdatedHistory.alias("h1").join(
      UpdatedHistory.alias("h2"),
      col("h1.id")===col("h2.id") && col("h2.moved_in")>col("h1.moved_in")
    ).groupBy("h1.id","h1.moved_in").agg(
      min("h2.moved_in").alias("next_moved_in")
    )
    val OrderedUpdatedHistory = UpdatedHistory.join(Ordering_Moved_IN,Seq("id","moved_in"),"left")
      .withColumn(
        "moved_out",
        when(
          col("next_moved_in").isNotNull,
          col("next_moved_in")
        ).otherwise(currentDate)
      ).drop("next_moved_in")
      .select(column_names.map(col):_*)
      .orderBy(col("id"),col("moved_in")).distinct()
    OrderedUpdatedHistory
  }
  def SetOnlyLatestRecordTrue(OrderedUpdatedHistory:DataFrame): DataFrame = {
    val MaxMovedIN = OrderedUpdatedHistory.groupBy("id").agg(max("moved_in").alias("max_moved_in"))
    val history = OrderedUpdatedHistory.join(MaxMovedIN,Seq("id"))
      .withColumn("is_latest",col("moved_in")===col("max_moved_in"))
      .withColumn("current",when(col("is_latest")===false,false).otherwise(true))
      .drop("max_moved_in")
      .drop("is_latest")
    history.orderBy(col("id"),col("moved_in"))
  }
}
