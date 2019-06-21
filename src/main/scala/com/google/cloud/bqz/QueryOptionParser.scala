/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.bqz

import scopt.OptionParser

object QueryOptionParser extends OptionParser[QueryConfig]("query"){
  def parse(args: Seq[String]): Option[QueryConfig] =
    parse(args, QueryConfig())

  head("query")

  help("help")
    .text("prints this usage text")

  /*
  opt[Boolean]("allow_large_results")
    .text("When specified, enables large destination table sizes for legacy SQL queries.")
  */

  opt[Boolean]("append_table")
    .text("When specified, append data to a destination table. The default value is false.")
    .action((x,c) => c.copy(appendTable = x))

  opt[Boolean]("batch")
    .text("When specified, run the query in batch mode. The default value is false.")
    .action((x,c) => c.copy(batch = x))

  opt[Seq[String]]("clustering_fields")
    .text("If specified, a comma-separated list of columns is used to cluster the destination table in a query. This flag must be used with the time partitioning flags to create either an ingestion-time partitioned table or a table partitioned on a DATE or TIMESTAMP column. When specified, the table is first partitioned, and then it is clustered using the supplied columns.")
    .action((x,c) => c.copy(clusteringFields = x))

  opt[String]("destinationKmsKey")
    .text("The Cloud KMS key used to encrypt the destination table data.")
    .action((x,c) => c.copy(destinationKmsKey = x))

  opt[String]("destinationSchema")
    .text("The path to a local JSON schema file or a comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]. The default value is ''.")
    .action((x,c) => c.copy(destinationSchema = x))

  opt[String]("destinationTable")
    .text("The name of the destination table for writing query results. The default value is ''")
    .action((x,c) => c.copy(destinationTable = x))

  opt[Boolean]("dryRun")
    .text("When specified, the query is validated but not run.")
    .action((x,c) => c.copy(dryRun = x))

  opt[String]("externalTableDefinition")
    .text("The table name and schema definition used in an external table query. The schema can be a path to a local JSON schema file or a comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]. The format for supplying the table name and schema is: [TABLE]::[PATH_TO_FILE] or [TABLE]::[SCHEMA]@[SOURCE_FORMAT]=[CLOUD_STORAGE_URI]. Repeat this flag to query multiple tables.")
    .action((x,c) => c.copy(externalTableDefinition = x))

  /*
  opt[Boolean]("flattenResults")
    .text("When specified, flatten nested and repeated fields in the results for legacy SQL queries. The default value is true.")
  */

  opt[String]("label")
    .text("A label to apply to a query job in the form [KEY]:[VALUE]. Repeat this flag to specify multiple labels.")
    .action((x,c) => c.copy(label = x))

  /*
  opt[Int]('n', "maxRows")
    .text("An integer specifying the number of rows to return in the query results. The default value is 100.")
  */

  opt[Long]("maximum_bytes_billed")
    .text("An integer that limits the bytes billed for the query. If the query goes beyond the limit, it fails (without incurring a charge). If not specified, the bytes billed is set to the project default.")
    .action((x,c) => c.copy(maximumBytesBilled = x))

  /*
  opt[Double]("minCompletionRatio")
    .text("[Experimental] A number between 0 and 1.0 that specifies the minimum fraction of data that must be scanned before a query returns. If not set, the default server value 1.0 is used.")
  */

  opt[Seq[String]]("parameters")
    .text("comma-separated query parameters in the form [NAME]:[TYPE]:[VALUE]. An empty name creates a positional parameter. [TYPE] may be omitted to assume a STRING value in the form: name::value or ::value. NULL produces a null value.")
    .validate{x =>
      if (x.exists(_.split(':').length != 3))
        failure("parameter must be in the form [NAME]:[TYPE]:[VALUE]")
      else
        success
    }
    .action((x,c) => c.copy(parameters = x))

  opt[Boolean]("replace")
    .text("If specified, overwrite the destination table with the query results. The default value is false.")
    .action((x,c) => c.copy(replace = x))

  opt[Boolean]("require_cache")
    .text("If specified, run the query only if results can be retrieved from the cache.")
    .action((x,c) => c.copy(requireCache = x))

  opt[Boolean]("require_partition_filter")
    .text("If specified, a partition filter is required for queries over the supplied table. This flag can only be used with a partitioned table.")
    .action((x,c) => c.copy(requirePartitionFilter = x))

  /*
  opt[Boolean]("rpc")
    .text("If specified, use the rpc-style query API instead of the REST API jobs.insert method. The default value is false.")
  */

  opt[Seq[String]]("schema_update_option")
    .text("When appending data to a table (in a load job or a query job), or when overwriting a table partition, specifies how to update the schema of the destination table. Possible values include:\n\n ALLOW_FIELD_ADDITION: Allow\nnew fields to be added\n ALLOW_FIELD_RELAXATION: Allow relaxing REQUIRED fields to NULLABLE")
    .action((x,c) => c.copy(schemaUpdateOption = x))

  /*
  opt[Int]('s', "startRow")
    .text("An integer that specifies the first row to return in the query result. The default value is 0.")
  */

  opt[Long]("time_partitioning_expiration")
    .text("An integer that specifies (in seconds) when a time-based partition should be deleted. The expiration time evaluates to the partition's UTC date plus the integer value. A negative number indicates no expiration.")
    .action((x,c) => c.copy(timePartitioningExpiration = x))

  opt[String]("time_partitioning_field")
    .text("The field used to determine how to create a time-based partition. If time-based partitioning is enabled without this value, the table is partitioned based on the load time.")
    .action((x,c) => c.copy(timePartitioningField = x))

  opt[String]("time_partitioning_type")
    .text("Enables time-based partitioning on a table and sets the partition type. Currently, the only possible value is DAY which generates one partition per day.")
    .action((x,c) => c.copy(timePartitioningType = x))

  /*
  opt[String]("udfResource")
    .text("This flag applies only to legacy SQL queries. When specified, this is the Cloud Storage URI or the path to a local code file that is loaded and evaluated immediately as a user-defined function resource used by a legacy SQL query. Repeat this flag to specify multiple files.")
  */

  opt[Boolean]("use_cache")
    .text("When specified, caches the query results. The default value is true.")
    .action((x,c) => c.copy(useCache = x))

  opt[Boolean]("use_legacy_sql")
    .text("When set to false, runs a standard SQL query. The default value is false (uses Standard SQL).")
    .action((x,c) => c.copy(useLegacySql = x))

  // Global options

  opt[String]("dataset_id")
    .text(GlobalConfig.datasetIdText)
    .action((x,c) => c.copy(datasetId = x))

  opt[Boolean]("debug_mode")
    .text(GlobalConfig.debugModeText)
    .action((x,c) => c.copy(debugMode = x))

  opt[String]("job_id")
    .text(GlobalConfig.jobIdText)
    .action((x,c) => c.copy(jobId = x))

  opt[String]("location")
    .text(GlobalConfig.locationText)
    .action((x,c) => c.copy(location = x))

  opt[String]("project_id")
    .text(GlobalConfig.projectIdText)
    .action((x,c) => c.copy(projectId = x))

  opt[Boolean]("synchronous_mode")
    .text(GlobalConfig.synchronousModeText)
    .action((x,c) => c.copy(synchronousMode = x))

  opt[Boolean]("sync")
    .text(GlobalConfig.syncText)
    .action((x,c) => c.copy(sync = x))
}
