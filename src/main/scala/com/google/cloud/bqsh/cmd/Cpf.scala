package com.google.cloud.bqsh.cmd

import com.google.cloud.bqsh.{ArgParser, Command, GsUtilConfig, GsUtilConfigF, MkConfig}
import com.google.cloud.imf.gzos.MVS
import com.google.cloud.imf.util.Logging
import com.google.gson.Gson

object Cpf extends Command[GsUtilConfig] with Logging {

  override val name: String = _
  override val parser: ArgParser[GsUtilConfig] = _

  override def run(config: GsUtilConfigF, zos: MVS): Result = {

    // Copy the files & Upload to GCS
    if (Cp.run(config, zos).exitCode == 0) {
      // Create external temporary table

      val stageTable = zos.jobName + "_" + zos.jobId + "_"
      //      val gcsPath = InterpreterEnv.stagingPath(env.gcsPrefix, jobInfo, zos.getDSN(ddName))
//      val stagingTable = InterpreterEnv.tableName(jobInfo, ddName)

      // Register as BigQuery External Table
      val mkConfig = MkConfig.create(config.gcsUri, config.datasetId + "." + stageTable,config.datasetId, config.location, config.projectId)

      Mk.run(mkConfig,zos ) ///

      // Create the select from the statement
      val g = new Gson()
      val f: Filter = g.fromJson(config.filter, Filter.getClass)
    }
  }
}


case class Filter(fcol: Seq[FilteredColumn])


case class FilteredColumn(name: String = "",
                           castFromType: String = "",
                           castToType: String = "",
                           ifNull: String = "",
                           castFromFormat: String,
                           castToFormat: String,
                          trim: Boolean)
