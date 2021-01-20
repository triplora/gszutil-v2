package com.google.cloud.gzos

import com.google.cloud.imf.gzos.DataSetInfo
import org.scalatest.flatspec.AnyFlatSpec

class DataSetInfoSpec extends AnyFlatSpec {

  case class ExpectedDataSetInfo(elementName: String, pds: Boolean, gdg: Boolean, objectName: String)

  val DsnToExpectedDataSetInfo = Map(
    "N1.R6.MDS" -> ExpectedDataSetInfo("", false, false, "N1.R6.MDS"),
    "N1.R6.MDS(0)" -> ExpectedDataSetInfo("0", false, false, "N1.R6.MDS(0)"),
    "N1.R6.MDS(TD11)" -> ExpectedDataSetInfo("TD11", true, false, "N1.R6.MDS/TD11"),
    "N1.R6.MDS.G1234V56" -> ExpectedDataSetInfo("", false, true, "N1.R6.MDS"),
  )

  "DataSetInfoSpec" should "match expected inputs" in {
    DsnToExpectedDataSetInfo.foreach(dsnToExpectedDs =>{
      val actualDs = DataSetInfo(dsnToExpectedDs._1)
      assert(actualDs.elementName == dsnToExpectedDs._2.elementName)
      assert(actualDs.gdg == dsnToExpectedDs._2.gdg)
      assert(actualDs.pds == dsnToExpectedDs._2.pds)
      assert(actualDs.objectName == dsnToExpectedDs._2.objectName)
    })
  }
}
