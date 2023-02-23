import RenovateHistoryData.renovateHistory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate
case class HistoryData(id:Int, firstname:String, lastname:String, address:String, moved_in:Date, moved_out:Date, current:Boolean)
case class UpdatesData(uid:Int, ufirstname:String, ulastname:String, uaddress:String, umoved_in:Date)
class RenovateHistorySpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("First test")
    .getOrCreate()
  import spark.implicits._
  val currentDate = LocalDate.now
  "renovateHistory" should "Insert data from Updates to History when there's new records" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1992-02-01"), Date.valueOf("2000-02-12"), false),
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("2000-02-12"), Date.valueOf(currentDate), true)
    )
    val updatesDetails = Seq(
      UpdatesData(6, "Aziz", "Maamar", "Paris", Date.valueOf("2023-01-01")),
      UpdatesData(7, "Haythem", "Selmi", "Tunis", Date.valueOf("2015-08-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF

    When("updateTable is Invoked")
    val UpdatedHistory = renovateHistory(Updates,History)

    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(6, "Aziz", "Maamar", "Paris", Date.valueOf("2023-01-01"), Date.valueOf(currentDate), true),
      HistoryData(7, "Haythem", "Selmi", "Tunis", Date.valueOf("2015-08-01"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1992-02-01"), Date.valueOf("2000-02-12"), false),
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("2000-02-12"), Date.valueOf(currentDate), true)
    ).toDF
    UpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "renovateHistory" should "store coming data of a new person ordered" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true)
    )
    val updatesDetails = Seq(
      UpdatesData(6, "Aziz", "Maamar", "Thala", Date.valueOf("1996-01-01")),
      UpdatesData(6, "Aziz", "Maamar", "Sousse", Date.valueOf("1995-01-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF

    When("updateTable is Invoked")
    val UpdatedHistory = renovateHistory(Updates, History)

    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true),
      HistoryData(6, "Aziz", "Maamar", "Sousse", Date.valueOf("1995-01-01"), Date.valueOf("1996-01-01"), false),
      HistoryData(6, "Aziz", "Maamar", "Thala", Date.valueOf("1996-01-01"), Date.valueOf(currentDate), true),
    ).toDF
    UpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "renovateHistory" should "Insert a new record and Expire the existing record when a person is changing his address" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("2000-02-12"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1992-02-12"), Date.valueOf("2000-02-12"), false)
    )
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2016-01-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF

    When("updateTable is Invoked")
    val UpdatedHistory = renovateHistory(Updates, History)

    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("2016-01-01"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("2000-02-12"), Date.valueOf("2016-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1992-02-12"), Date.valueOf("2000-02-12"), false)
    ).toDF
    UpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "renovateHistory" should "Do nothing if the values did not change" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("2000-02-12"), Date.valueOf(currentDate), true)
    )
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("2016-01-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF

    When("updateTable is Invoked")
    val UpdatedHistory = renovateHistory(Updates, History)

    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("2000-02-12"), Date.valueOf(currentDate), true)
    ).toDF
    UpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "renovateHistory" should "Insert new records when there are multiple updates for a certain person" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("2000-01-01"), false)
    )
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2022-01-05")),
      UpdatesData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF

    When("updateTable is Invoked")
    val UpdatedHistory = renovateHistory(Updates, History)
    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1990-01-05"), false),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"), Date.valueOf("2000-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("2022-01-05"), Date.valueOf(currentDate), true)
    ).toDF
    UpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "renovateHistory" should "Insert new records to History with false as current if the date " +
    "of the new records are lesser than the existing record for a certain person" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("1990-12-05"), Date.valueOf("1997-12-05"), false)
    )
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("1995-01-01")),
      UpdatesData(5, "Houssem", "Abidi", "Thala", Date.valueOf("1996-01-01"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF

    When("updateTable is Invoked")
    val UpdatedHistory = renovateHistory(Updates, History)

    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1997-12-05"), Date.valueOf(currentDate), true),
      HistoryData(5, "Houssem", "Abidi", "Sousse", Date.valueOf("1995-01-01"), Date.valueOf("1996-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Thala", Date.valueOf("1996-01-01"), Date.valueOf("1997-12-05"), false),
      HistoryData(5, "Houssem", "Abidi", "Ariana", Date.valueOf("1990-12-05"), Date.valueOf("1995-01-01"), false),
    ).toDF
    UpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "renovateHistory" should "Update a person's data even in the further past" in {
    Given("The input Data")
    val historyDetails = Seq(
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("2000-01-01"), false)
    )
    val updatesDetails = Seq(
      UpdatesData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"))
    )
    val History = historyDetails.toDF
    val Updates = updatesDetails.toDF

    When("updateTable is Invoked")
    val UpdatedHistory = renovateHistory(Updates, History)

    Then("The Updated Table should be returned")
    val expectedResult = Seq(
      HistoryData(5, "Houssem", "Abidi", "Kasserine", Date.valueOf("1960-12-05"), Date.valueOf("1970-01-01"), false),
      HistoryData(5, "Houssem", "Abidi", "Tunis", Date.valueOf("1980-12-05"), Date.valueOf("1990-01-05"), false),
      HistoryData(5, "Houssem", "Abidi", "Feriana", Date.valueOf("1990-01-05"), Date.valueOf("2000-01-01"), false)
    ).toDF
    UpdatedHistory.collect() should contain theSameElementsAs expectedResult.collect()
  }
}
