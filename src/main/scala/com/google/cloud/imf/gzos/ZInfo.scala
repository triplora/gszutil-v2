package com.google.cloud.imf.gzos

import com.google.cloud.imf.gzos.pb.GRecvProto.ZOSJobInfo

case class ZInfo(jobId: String = "",
                 jobName: String = "",
                 jobDate: String = "",
                 jobTime: String = "",
                 stepName: String = "",
                 procStepName: String = "",
                 user: String = "",
                 symbols: Map[String,String] = Map.empty) {
  def toJobInfo: ZOSJobInfo =
    ZOSJobInfo.newBuilder
      .setJobid(jobId)
      .setJobname(jobName)
      .setJobdate(jobDate)
      .setJobtime(jobTime)
      .setStepName(stepName)
      .setProcStepName(procStepName)
      .setUser(user)
      .build

  def toMap: java.util.Map[String,String] = {
    val content = new java.util.HashMap[String,String]()
    content.put("jobid", jobId)
    content.put("jobdate", jobDate)
    content.put("jobtime", jobTime)
    content.put("jobname", jobName)
    content.put("stepname", stepName)
    content.put("procstepname", procStepName)
    content.put("symbols", symbols.map(x => s"${x._1}=${x._2}").mkString("\n"))
    content.put("user", user)
    content
  }
}
