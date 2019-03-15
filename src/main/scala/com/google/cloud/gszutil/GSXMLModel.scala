/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gszutil

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper

object GSXMLModel {

  class CommonPrefixes {
    @JsonProperty("Prefix")
    var Prefix: String = ""
  }

  class CustomerEncryption {
    @JsonProperty("EncryptionAlgorithm")
    var EncryptionAlgorithm: String = ""

    @JsonProperty("KeySha256")
    var KeySha256: String = ""
  }

  class ListBucketResult {
    @JsonProperty("Name")
    var Name: String = ""

    @JacksonXmlElementWrapper(useWrapping = false)
    var CommonPrefixes: java.util.ArrayList[CommonPrefixes] = new java.util.ArrayList()

    @JsonProperty("Delimiter")
    var Delimiter: String = ""

    @JsonProperty("MaxKeys")
    var MaxKeys: Long = 0

    @JsonProperty("Prefix")
    var Prefix: String = ""

    @JsonProperty("GenerationMarker")
    var GenerationMarker: String = ""

    @JsonProperty("NextGenerationMarker")
    var NextGenerationMarker: String = ""

    @JsonProperty("Marker")
    var Marker: String = ""

    @JsonProperty("NextMarker")
    var NextMarker: String = ""

    @JsonProperty("IsTruncated")
    var IsTruncated: Boolean = false

    @JsonProperty("Contents")
    @JacksonXmlElementWrapper(useWrapping = false)
    var Contents: java.util.ArrayList[Contents] = new java.util.ArrayList()

    @JsonProperty("Version")
    @JacksonXmlElementWrapper(useWrapping = false)
    var Version: java.util.ArrayList[Contents] = new java.util.ArrayList()
  }

  class Contents {

    @JsonProperty("Key")
    var Key: String = ""

    @JsonProperty("Generation")
    var Generation: Long = 0

    @JsonProperty("MetaGeneration")
    var MetaGeneration: Long = 0

    @JsonProperty("IsLatest")
    var IsLatest: Boolean = true

    @JsonProperty("LastModified")
    var LastModified: String = ""

    @JsonProperty("DeletedTime")
    var DeletedTime: String = ""

    @JsonProperty("ETag")
    var ETag: String = ""

    @JsonProperty("Size")
    var Size: Long = 0

    @JsonProperty("KmsKeyName")
    var KmsKeyName: String = ""

    @JsonProperty("CustomerEncryption")
    var CustomerEncryption: CustomerEncryption = _
  }

}
