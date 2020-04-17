package com.google.cloud.imf.gzos

object MVSStorage {
  sealed trait DSN {
    def fqdsn: String
    override def toString: String = fqdsn
  }

  case class MVSDataset(dsn: String) extends DSN {
    override val fqdsn: String = s"//'$dsn'"
  }

  case class MVSPDSMember(pdsDsn: String, member: String) extends DSN {
    override val fqdsn: String = s"//'$pdsDsn($member)'"
  }
}
