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
}
