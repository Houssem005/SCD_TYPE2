import UpdatingTable.{ExtractNoActionData, InsertNewPeopleHistory, InsertOldPeopleHistory, OrderingHistoryDates, SetOnlyLatestRecordTrue, UpdateHistory, UpdateOldPeopleRecord, getJoinedHistoryAndUpdate}
import org.apache.spark.sql.DataFrame

object RenovateHistoryData {
  def RenovateHistory(Updates: DataFrame, History: DataFrame): DataFrame = {
    val updatesAndHistoryData = getJoinedHistoryAndUpdate(Updates,History)
    val InsertedRecords = InsertNewPeopleHistory(updatesAndHistoryData)
    val NoActionData = ExtractNoActionData(updatesAndHistoryData)
    val InsertedOldPeopleHistory = InsertOldPeopleHistory(updatesAndHistoryData)
    val UpdatedRecordHistory = UpdateOldPeopleRecord(updatesAndHistoryData)
    val UpdatedHistory = UpdateHistory(InsertedRecords,NoActionData,InsertedOldPeopleHistory,UpdatedRecordHistory)
    val OrderedUpdatedHistory =OrderingHistoryDates(UpdatedHistory)
    val RenovatedHistory = SetOnlyLatestRecordTrue(OrderedUpdatedHistory)
    RenovatedHistory
  }
}
