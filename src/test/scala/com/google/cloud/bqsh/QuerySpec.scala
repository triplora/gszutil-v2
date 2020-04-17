package com.google.cloud.bqsh

import com.google.cloud.imf.gzos.Util
import org.scalatest.flatspec.AnyFlatSpec

class QuerySpec extends AnyFlatSpec {
  Util.configureLogging(true)
  "Query" should "parse stats table" in {
    val example = "test-project-id:TEST_DATASET_A.TABLE_NAME"
    val resolved = BQ.resolveTableSpec(example,"","")
    assert(resolved.getProject == "test-project-id")
    assert(resolved.getDataset == "TEST_DATASET_A")
    assert(resolved.getTable == "TABLE_NAME")
  }
}
