package com.google.cloud.gszutil

final case class BigQueryConfig(
                                 project: String = "",
                                 dataset: String = "",
                                 table: String = "",
                                 bucket: String = "",
                                 path: String = "",
                                 location: String = "US")
