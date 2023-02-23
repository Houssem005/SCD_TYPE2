import Utils.{extractNoActionData, insertNewPeopleHistory, insertOldPeopleHistory, orderingHistoryDates, setOnlyLatestRecordTrue, updateHistory, updateOldPeopleRecord, getJoinedHistoryAndUpdate}
import org.apache.spark.sql.DataFrame

object RenovateHistoryData {
  def renovateHistory(Updates: DataFrame, History: DataFrame): DataFrame = {
    val UpdatesAndHistoryData = getJoinedHistoryAndUpdate(Updates,History)
    val InsertedRecords = insertNewPeopleHistory(UpdatesAndHistoryData)
    val NoActionData = extractNoActionData(UpdatesAndHistoryData)
    val InsertedOldPeopleHistory = insertOldPeopleHistory(UpdatesAndHistoryData)
    val UpdatedRecordHistory = updateOldPeopleRecord(UpdatesAndHistoryData)
    val UpdatedHistory = updateHistory(InsertedRecords,NoActionData,InsertedOldPeopleHistory,UpdatedRecordHistory)
    val OrderedUpdatedHistory =orderingHistoryDates(UpdatedHistory)
    val RenovatedHistory = setOnlyLatestRecordTrue(OrderedUpdatedHistory)
    RenovatedHistory
  }
}
