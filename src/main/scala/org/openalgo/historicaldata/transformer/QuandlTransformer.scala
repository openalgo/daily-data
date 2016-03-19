package org.openalgo.historicaldata.transformer

import java.io.StringReader

import argonaut.Argonaut.{casecodec1, casecodec5}
import com.github.tototoshi.csv.CSVReader

object QuandlTransformer {

  private val DATE_FIELD = "date"
  private val HIGH_FIELD = "high"
  private val LOW_FIELD = "low"
  private val OPEN_FIELD = "open"
  private val CLOSE_FIELD = "close"
  private val PRICE_DATA_FIELD = "priceData"

  private val DATE_FIELD_CAP = "Date"
  private val HIGH_FIELD_CAP = "High"
  private val LOW_FIELD_CAP = "Low"
  private val OPEN_FIELD_CAP = "Open"
  private val CLOSE_FIELD_CAP = "Close"


  case class PriceData(date: String, high: Double, low: Double, open: Double, close: Double)

  case class QuandlData(priceData: List[PriceData])

  implicit def PriceDataCodecJson =
    casecodec5(PriceData.apply, PriceData.unapply)(DATE_FIELD, HIGH_FIELD, LOW_FIELD, OPEN_FIELD, CLOSE_FIELD)

  implicit def QuandlDataCodecJson =
    casecodec1(QuandlData.apply, QuandlData.unapply)(PRICE_DATA_FIELD)

  def toDouble(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case err1: NumberFormatException => 0
    }
  }

  def transformQuandlData(csvStr: String): QuandlData = {
    val cSVReader = CSVReader.open(new StringReader(csvStr))
    val iterator = cSVReader.iteratorWithHeaders
    var accumList: List[PriceData] = List()
    while (iterator.hasNext) {
      val itr = iterator.next()
      if (itr.nonEmpty) {
        accumList = PriceData(itr.getOrElse(DATE_FIELD_CAP, "Nil"), toDouble(itr.getOrElse(HIGH_FIELD_CAP, "0")), toDouble(itr.getOrElse(LOW_FIELD_CAP, "0")),
          toDouble(itr.getOrElse(OPEN_FIELD_CAP, "0")), toDouble(itr.getOrElse(CLOSE_FIELD_CAP, "0"))) :: accumList
      }
    }
    //TODO csv has earliest data on top, Quandl API may have a setting to save CPU cycle
    QuandlData(accumList.reverse)
  }
}
