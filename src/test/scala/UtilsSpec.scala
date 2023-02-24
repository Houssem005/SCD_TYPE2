import Utils.{extractNoActionData, getJoinedHistoryAndUpdate, insertNewPeopleHistory, insertOldPeopleHistory, orderingHistoryDates, setOnlyLatestRecordTrue, updateHistory, updateOldPeopleRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, StringType, StructField, StructType}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate
case class UpdatesAndHistory(id: Option[Int], firstname: Option[String], lastname: Option[String],
                             address: Option[String], moved_in: Option[java.sql.Date],
                             moved_out: Option[java.sql.Date], current: Option[Boolean],
                             uid: Option[Int], ufirstname: Option[String], ulastname: Option[String],
                             uaddress: Option[String], umoved_in: Option[java.sql.Date],
                             umoved_out: Option[java.sql.Date], action: Option[String])
class UtilsSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("First test")
    .getOrCreate()
  import spark.implicits._
  val currentDate = LocalDate.now

  "getJoinedHistoryAndUpdate" should "add a moved_in column with current date to the update table " +
    "and give back History and the new update joined with UPSERT as an action when the address changes" in{
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true)
    )
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2020-01-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF
    When("getJoinedHistoryAndUpdate is Invoked")
    val UpdatesAndHistoryData = getJoinedHistoryAndUpdate(Updates,History)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      (5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true,5, "Houssem", "Abidi", "Tunis", Date.valueOf("2020-01-01"), Date.valueOf(currentDate),"UPSERT")
    ).toDF("id","firstname","lastname","address","moved_in","moved_out","current","uid","ufirstname","ulastname","uaddress","umoved_in","umoved_out","action")
    UpdatesAndHistoryData.collect() should contain theSameElementsAs expectedResult.collect()
  }
  "getJoinedHistoryAndUpdate" should "add a moved_in column with current date to the update table " +
    "give back History and Updates joined with INSERT as an action when there's a new record" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true)
    )
    val updatesDetails = Seq(
      UpdatesData(6, "Aziz", "Maamar", "Skhirat", Date.valueOf("2020-01-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF
    When("getJoinedHistoryAndUpdate is Invoked")
    val UpdatesAndHistoryData = getJoinedHistoryAndUpdate(Updates, History)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      UpdatesAndHistory(Some(5), Some("Houssem"), Some("Abidi"), Some("Feriana"), Some(Date.valueOf("1997-12-05")), Some(Date.valueOf(currentDate)), Some(true), None, None, None, None, None, None, Some("NOACTION")),
      UpdatesAndHistory(None, None, None, None, None, None, None, Some(6), Some("Aziz"), Some("Maamar"), Some("Skhirat"), Some(Date.valueOf("2020-01-01")), Some(Date.valueOf(currentDate)), Some("INSERT"))
    ).toDF()
    UpdatesAndHistoryData.collect() should contain theSameElementsAs expectedResult.collect()
  }
  "getJoinedHistoryAndUpdate" should "add a moved_in column with current date to the update table" +
    "give back History and Updates joined with NOACTION as an action when the address did not change" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true)
    )
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("2020-01-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF
    When("getJoinedHistoryAndUpdate is Invoked")
    val UpdatesAndHistoryData = getJoinedHistoryAndUpdate(Updates, History)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      UpdatesAndHistory(Some(5), Some("Houssem"), Some("Abidi"), Some("Feriana"), Some(Date.valueOf("1997-12-05")), Some(Date.valueOf(currentDate)), Some(true), Some(5), Some("Houssem"), Some("Abidi"), Some("Feriana"), Some(Date.valueOf("2020-01-01")), Some(Date.valueOf(currentDate)), Some("NOACTION"))
    ).toDF()
    UpdatesAndHistoryData.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "insertNewPeopleHistory" should "extract new records to be inserted when the action is INSERT" in {
    Given("The input Data")
    val UpdatesAndHistoryData = Seq(
      UpdatesAndHistory(Some(5), Some("Houssem"), Some("Abidi"), Some("Feriana"), Some(Date.valueOf("1997-12-05")), Some(Date.valueOf("2023-02-23")), Some(true), None, None, None, None, None, None, Some("NOACTION")),
      UpdatesAndHistory(None, None, None, None, None, None, None, Some(6), Some("Aziz"), Some("Maamar"), Some("Skhirat"), Some(Date.valueOf("2020-01-01")), Some(Date.valueOf(currentDate)), Some("INSERT"))
    ).toDF()
    When("insertNewPeopleHistory is Invoked")
    val InsertedRecords = insertNewPeopleHistory(UpdatesAndHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(6,"Aziz","Maamar","Skhirat",Date.valueOf("2020-01-01"),Date.valueOf(currentDate),true)
    ).toDF()
    InsertedRecords.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "extractNoActionData" should "extract records that needs no action " +
    "and maintain the same moved_in date if the existing date is superior to the coming date" in {
    Given("The input Data")
    val UpdatesAndHistoryData = Seq(
      UpdatesAndHistory(Some(5), Some("Houssem"), Some("Abidi"), Some("Feriana"), Some(Date.valueOf("1997-12-05")),
        Some(Date.valueOf(currentDate)), Some(true),
        Some(5), Some("Houssem"), Some("Abidi"),
        Some("Feriana"), Some(Date.valueOf("2020-01-01")),
        Some(Date.valueOf(currentDate)), Some("NOACTION"))
    ).toDF()
    When("insertNewPeopleHistory is Invoked")
    val NoActionData = extractNoActionData(UpdatesAndHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true)
    ).toDF()
    NoActionData.collect() should contain theSameElementsAs expectedResult.collect()
  }
  "extractNoActionData" should "extract records that needs no action " +
    "and only change the moved_in date if the existing date is inferior to the coming date" in {
    Given("The input Data")
    val UpdatesAndHistoryData = Seq(
      UpdatesAndHistory(Some(5), Some("Houssem"), Some("Abidi"), Some("Feriana"), Some(Date.valueOf("1997-12-05")),
        Some(Date.valueOf(currentDate)), Some(true),
        Some(5), Some("Houssem"), Some("Abidi"),
        Some("Feriana"), Some(Date.valueOf("1990-01-01")),
        Some(Date.valueOf(currentDate)), Some("NOACTION"))
    ).toDF()
    When("insertNewPeopleHistory is Invoked")
    val NoActionData = extractNoActionData(UpdatesAndHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-01"), Date.valueOf(currentDate), true)
    ).toDF()
    NoActionData.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "insertOldPeopleHistory" should "extract existing people's records that needs to be inserted when the action is UPSERT " in {
    Given("The input Data")
    val UpdatesAndHistoryData = Seq(
    UpdatesAndHistory(Some(5), Some("Houssem"), Some("Abidi"), Some("Feriana"), Some(Date.valueOf("1997-12-05")),
      Some(Date.valueOf(currentDate)), Some(true),
      Some(5), Some("Houssem"), Some("Abidi"), Some("Tunis"),
      Some(Date.valueOf("2020-01-01")),
      Some(Date.valueOf(currentDate)), Some("UPSERT"))
    ).toDF()
    When("insertNewPeopleHistory is Invoked")
    val OldPeopleHistory = insertOldPeopleHistory(UpdatesAndHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2020-01-01"), Date.valueOf(currentDate), true)
    ).toDF()
    OldPeopleHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateOldPeopleRecord" should "extract existing people records and change their moved_out date " +
    "if the coming date is superior than the actual one and update the status " in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("1997-12-05"), Date.valueOf("2000-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2000-12-05"), Date.valueOf(currentDate), true)
    ).toDF
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Gafsa", Date.valueOf("2002-01-05"))
    ).toDF
    val UpdatesAndHistoryData = getJoinedHistoryAndUpdate(updatesDetails,historyDetails)
    When("updateOldPeopleRecord is Invoked")
    val UpdatedRecordHistoryData = updateOldPeopleRecord(UpdatesAndHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("1997-12-05"), Date.valueOf("2000-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2000-12-05"), Date.valueOf("2002-01-05"), false)
    ).toDF()
    UpdatedRecordHistoryData.collect() should contain theSameElementsAs expectedResult.collect()
  }
  "updateOldPeopleRecord" should "only extract existing people that needs to be updated" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("1997-12-05"), Date.valueOf("2000-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2000-12-05"), Date.valueOf("2002-01-05"), false)
    ).toDF
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Gafsa", Date.valueOf("2016-01-05"))
    ).toDF
    val UpdatesAndHistoryData = getJoinedHistoryAndUpdate(updatesDetails, historyDetails)
    When("updateOldPeopleRecord is Invoked")
    val UpdatedRecordHistoryData = updateOldPeopleRecord(UpdatesAndHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("1997-12-05"), Date.valueOf("2000-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2000-12-05"), Date.valueOf("2002-01-05"), false)
    ).toDF()
    UpdatedRecordHistoryData.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "orderingHistoryDates" should "sort the moved_in and moved_out dates of the updated history " +
    "after unionizing new inserted records,no action data, existing people inserted records and existing people updated records" in {
    Given("The input Data")
    val History = Seq(
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1995-01-01"), false)
    ).toDF
    val Updates = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2000-01-05")),
      UpdatesData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"))
    ).toDF
    val UpdatesAndHistoryData = getJoinedHistoryAndUpdate(Updates, History)
    val InsertedRecords = insertNewPeopleHistory(UpdatesAndHistoryData)
    val NoActionData = extractNoActionData(UpdatesAndHistoryData)
    val InsertedOldPeopleHistory = insertOldPeopleHistory(UpdatesAndHistoryData)
    val UpdatedRecordHistory = updateOldPeopleRecord(UpdatesAndHistoryData)
    val UpdatedHistory = updateHistory(InsertedRecords, NoActionData, InsertedOldPeopleHistory, UpdatedRecordHistory)

    When("orderingHistoryDates is Invoked")
    val OrderedUpdatedHistory = orderingHistoryDates(UpdatedHistory)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2000-01-05"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"), Date.valueOf("1995-01-01"), true),
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1990-01-05"), false)
    ).toDF()
    OrderedUpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "setOnlyLatestRecordTrue" should "change only the record of a certain person form the new ordered history" +
    " with the moved_out as current date to true and all the others to false " in {
    Given("The input Data")
    val OrderedHistoryData = Seq(
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2000-01-05"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"), Date.valueOf("1995-01-01"), true),
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1990-01-05"), false)
    ).toDF()
    When("setOnlyLatestRecordTrue is Invoked")
    val RenovatedHistory = setOnlyLatestRecordTrue(OrderedHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2000-01-05"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"), Date.valueOf("1995-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1990-01-05"), false)
    ).toDF()
    RenovatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }
  "setOnlyLatestRecordTrue" should "change all the records of the new ordered history as false " +
    "if the moved_out is different than current date" in {
    Given("The input Data")
    val OrderedHistoryData = Seq(
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2000-01-05"), Date.valueOf("2005-01-01"), true),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"), Date.valueOf("1995-01-01"), true),
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1990-01-05"), false)
    ).toDF()
    When("setOnlyLatestRecordTrue is Invoked")
    val RenovatedHistory = setOnlyLatestRecordTrue(OrderedHistoryData)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2000-01-05"), Date.valueOf("2005-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"), Date.valueOf("1995-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1990-01-05"), false)
    ).toDF()
    RenovatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

}
