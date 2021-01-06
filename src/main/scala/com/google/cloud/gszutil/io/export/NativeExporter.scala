package com.google.cloud.gszutil.io.`export`

import com.google.cloud.bigquery.Job
import com.google.cloud.bqsh.cmd.Result

trait NativeExporter {
  def exportData(job: Job): Result
  def close(): Unit
}
