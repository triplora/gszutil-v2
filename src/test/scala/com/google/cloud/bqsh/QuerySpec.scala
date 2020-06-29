package com.google.cloud.bqsh

import com.google.cloud.imf.util.CloudLogging
import org.scalatest.flatspec.AnyFlatSpec

class QuerySpec extends AnyFlatSpec {
  CloudLogging.configureLogging(debugOverride = false)
  "Query" should "parse stats table" in {
    val example = "test-project-id:TEST_DATASET_A.TABLE_NAME"
    val resolved = BQ.resolveTableSpec(example,"","")
    assert(resolved.getProject == "test-project-id")
    assert(resolved.getDataset == "TEST_DATASET_A")
    assert(resolved.getTable == "TABLE_NAME")
  }
}
