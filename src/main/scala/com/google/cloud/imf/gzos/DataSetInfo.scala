/*
 * Copyright 2020 Google LLC All Rights Reserved.
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

package com.google.cloud.imf.gzos

/**
  *
  * @param dataSetName Data Set Name (DSNAME)
  * @param elementName Member Name (PDS) or Relative Generation Number (GDG)
  * @param lrecl Record Length
  * @param undefined Record Format flag
  * @param fixed Record Format flag
  * @param variable Record Format flag
  * @param blocked Record Format flag
  * @param fixedBlock Record Format flag
  * @param indexedSequential Data Set Organization type
  * @param physicalSequential Data Set Organization type
  * @param directAccess Data Set Organization type
  * @param partitioned  Data Set Organization type
  * @param located Located flag
  * @param gdg Generational Data Set
  * @param pds Partitioned Data Set
  */
case class DataSetInfo(
  dataSetName: String = "",
  elementName: String = "",
  lrecl: Int = -1,

  undefined: Boolean = false,
  fixed: Boolean = false,
  variable: Boolean = false,
  blocked: Boolean = false,
  fixedBlock: Boolean = false,

  indexedSequential: Boolean = false,
  physicalSequential: Boolean = false,
  directAccess: Boolean = false,
  partitioned: Boolean = false,

  located: Boolean = false,
  gdg: Boolean = false,
  pds: Boolean = false
)
