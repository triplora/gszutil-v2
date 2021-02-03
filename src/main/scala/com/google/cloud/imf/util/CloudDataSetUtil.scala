package com.google.cloud.imf.util

object CloudDataSetUtil {

  def isGdg(inputDsn: String): Boolean = {
    val i = inputDsn.lastIndexOf('.')
    i > -1 &&
      inputDsn.length - i == 9 &&
      inputDsn.charAt(i+1) == 'G' &&
      inputDsn.charAt(i+6) == 'V'
  }

  def extractGDGVersion(dataSetName : String): Option[String] = {
    if(isGdg(dataSetName)) {
      Option(dataSetName.substring(dataSetName.lastIndexOf('.'))).map(s => s.substring(1, 9))
    } else {
      None
    }
  }
}
