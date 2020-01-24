package com.google.cloud.bqsh

case class JCLUtilConfig(src: String = "",
                         dest: String = "",
                         transform: String = "",
                         limit: Int = 4096)
