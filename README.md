# GSZUtil

GSZUtil uploads data to Google Cloud Storage from a z/OS batch job.

(Experimental)


## Pre-Requisites

* [IBM SDK for z/OS, Java Technology Edition, Version 8](https://developer.ibm.com/javasdk/support/zos/)
* [SBT](https://www.scala-sbt.org/download.html)


## Installation

1. Extract IBM JDK to `/opt/J8.0_64` using [pax](https://www.ibm.com/support/knowledgecenter/en/ssw_aix_72/com.ibm.aix.cmds4/pax.htm)
2. Modify [build.sbt](build.sbt) with the path to `jzos.jar` (`/opt/J8.0_64/lib/ext/ibmjzos.jar` by default)
3. Modify [Credentials](src/main/scala/com/google/cloud/gszutil/Credentials.scala) and add your service account email and private key PEM copied from JSON keyfile.
3. Run `sbt assembly` from the repository root to build an assembly jar
4. Copy the assembly jar to z/OS filesystem
5. Copy service account json keyfile to `$HOME/.config/gcloud/application_default_credentials.json` on z/OS filesystem
6. Update `DSN`, `BUCKET`, `APP_JAR`, and `GOOGLE_APPLICATION_CREDENTIALS` variables in [GSZUTILPRC](GSZUTILPRC)
7. Convert `GSZUTILPRC` and `JVMPRC86` to EBCDIC with `iconv -t EBCDICUS -f UTF-8`
8. Copy `GSZUTILPRC` and `JVMPRC86` to z/OS with `sftp`
9. Submit `GSZUTILJCL` as an MVS job


### Install Script

```sh
#!/bin/bash

# Install pax utility
sudo apt install -y pax

# Extract IBM JDK
openssl sha1 SDK8_64bit_SR5_FP30.PAX.Z
# SHA1(SDK8_64bit_SR5_FP30.PAX.Z)= 5f072d2a2c09479f761b1b68a7d568c9248d9de1
gunzip -c SDK8_64bit_SR5_FP30.PAX.Z | sudo pax -r
openssl sha1 J8.0_64/lib/ext/ibmjzos.jar
# SHA1(J8.0_64/lib/ext/ibmjzos.jar)= 843b870a22853a146f91fd984a4670da2fedbd9a
sudo cp -a J8.0_64 /opt/

# Download SBT and install
wget https://sbt-downloads.cdnedge.bluemix.net/releases/v1.2.8/sbt-1.2.8.tgz
cd /opt
sudo tar xfzo ~/sbt-1.2.8.tgz
echo 'PATH="$PATH:/opt/sbt/bin"' >> ~/.profile && . ~/.profile

# Clone repository
cd ~
git clone https://github.com/jasonmar/gszutil
cd gszutil

# Edit Credentials.scala using a text editor
# (command not shown)

# Build assembly jar
sbt assembly

# Copy assembly jar to mainframe
# (command not shown)
# jar is located at ~/gszutil/target/scala-2.11/gszutil.jar
```


## Encoding Conversion

The examples below demonstrate conversion between [EBCDIC](https://www.ibm.com/support/knowledgecenter/zosbasics/com.ibm.zos.zappldev/zappldev_14.htm) and UTF-8.


### Convert EBCDIC to UTF-8

```sh
iconv -f EBCDICUS -t UTF-8 GSZUTILPRC | tr '\205' '\n' | tr -d '\302' | tr -cd '\11\12\15\40-\176' > GSZUTIL.PRC
iconv -f EBCDICUS -t UTF-8 GSZUTILJCL | tr '\205' '\n' | tr -d '\302' | tr -cd '\11\12\15\40-\176' > GSZUTIL.JCL
```

### Convert UTF-8 to EBCDIC

```sh
iconv -t EBCDICUS -f UTF-8 GSZUTIL.PRC > GSZUTILPRC
```

## Documentation

[Data Set Names](https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.idad400/name.htm)

A data set name can be from one to a series of twenty-two joined name segments. Each name segment represents a level of qualification.

Each name segment (qualifier) is 1 to 8 characters, the first of which must be alphabetic (A to Z) or national (# @ $). The remaining seven characters are either alphabetic, numeric (0 - 9), national, a hyphen (-). Name segments are separated by a period (.).

Data set names must not exceed 44 characters, including all name segments and periods.

A low-level qualifier GxxxxVyy, in which xxxx and yy are numbers is used for names of generation data sets.


[Working with Data Sets](https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.idad400/ch1.htm)


[Direct Access Storage Device Architecture](https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.idad400/dasda.htm#dasda)

[DD Statement](https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.ieab600/ddst.htm)

DD statement (data definition) is used to describe a data set and specify input and output resources. [more info](https://www.ibm.com/support/knowledgecenter/zosbasics/com.ibm.zos.zjcl/zjclc_jclDDstmt.htm)

The [DSNAME](https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.ieab600/xdddsn.htm) parameter specifies a data set name for input or output.

The [*](https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.ieab600/xddaster.htm) parameter begins an in-stream data set. Terminated with `/*` or `//`. 


### Example Help Text

```
GSZUtil 0.1.1
Usage: GSZUtil [load|cp|get] [options] <args>...

  --help                   prints this usage text
Command: load bqProject bqDataset bqTable bucket prefix
loads a BigQuery Table
  bqProject                BigQuery Project ID
  bqDataset                BigQuery Dataset name
  bqTable                  BigQuery Table name
  bucket                   GCS bucket of source
  prefix                   GCS prefix of source
Command: cp dest
GSZUtil cp copies a zOS dataset to GCS
  dest                     destination path (gs://bucket/path)
Command: get source dest
download a GCS object to UNIX filesystem
  source                   source path (/path/to/file)
  dest                     destination path (gs://bucket/path)
  --debug <value>          enable debug options (default: false)
  --partSizeMB <value>     target part size in megabytes (default: 256)
  --batchSize <value>      rows per batch (default: 10000)
  -p, --parallelism <value>
                           number of concurrent writers (default: 5)
  --timeOutMinutes <value>
                           timeout in minutes (default: 180)
```

## BQZ

### Load

```
load
Usage: load [options] tablespec path [schema]

  --help                   prints this usage text
  tablespec                Tablespec in format [PROJECT]:[DATASET].[TABLE]
  path                     Comma-separated source URIs in format gs://bucket/path,gs://bucket/path
  schema                   Comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE].
  --allow_jagged_rows <value>
                           When specified, allow missing trailing optional columns in CSV data.
  --allow_quoted_newlines <value>
                           When specified, allow quoted newlines in CSV data.
  --autodetect <value>     When specified, enable schema auto-detection for CSV and JSON data.
  --destination_kms_key <value>
                           The Cloud KMS key for encryption of the destination table data.
  -E, --encoding <value>   The character encoding used in the data. Possible values include:
ISO-8859-1 (also known as Latin-1) UTF-8
  -F, --field_delimiter <value>
                           The character that indicates the boundary between columns in the data. Both \t and tab are allowed for tab delimiters.
  --ignore_unknown_values <value>
                           When specified, allow and ignore extra, unrecognized values in CSV or JSON data.
  --max_bad_records <value>
                           An integer that specifies the maximum number of bad records allowed before the entire job fails. The default value is 0. At most, five errors of any type are returned regardless of the --max_bad_records value.
  --null_marker <value>    An optional custom string that represents a NULL value in CSV data.
  --clustering_fields <value>
                           If specified, a comma-separated list of columns is used to cluster the destination table in a query. This flag must be used with the time partitioning flags to create either an ingestion-time partitioned table or a table partitioned on a DATE or TIMESTAMP column. When specified, the table is first partitioned, and then it is clustered using the supplied columns.
  --projection_fields <value>
                           If used with --source_format set to DATASTORE_BACKUP, indicates which entity properties to load from a Cloud Datastore export as a comma-separated list. Property names are case sensitive and must refer to top-level properties. The default value is ''. This flag can also be used with Cloud Firestore exports.
  --quote <value>          The quote character to use to enclose records. The default value is " which indicates no quote character.
  --replace <value>        When specified, existing data is erased when new data is loaded. The default value is false.
  --append_table <value>   When specified, append data to a destination table. The default value is false.
  --schema <value>         Comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE].
  --schema_update_option <value>
                           When appending data to a table (in a load job or a query job), or when overwriting a table partition, specifies how to update the schema of the destination table. Possible values include:
ALLOW_FIELD_ADDITION: Allow new fields to be added
ALLOW_FIELD_RELAXATION: Allow relaxing REQUIRED fields to NULLABLE
Repeat this flag to specify multiple schema update options.
  --skip_leading_rows <value>
                           An integer that specifies the number of rows to skip at the beginning of the source file.
  --source_format <value>  The format of the source data. Possible values include: CSV NEWLINE_DELIMITED_JSON AVRO DATASTORE_BACKUP PARQUET ORC
  --time_partitioning_expiration <value>
                           An integer that specifies (in seconds) when a time-based partition should be deleted. The expiration time evaluates to the partition's UTC date plus the integer value. A negative number indicates no expiration.
  --time_partitioning_field <value>
                           The field used to determine how to create a time-based partition. If time-based partitioning is enabled without this value, the table is partitioned based on the load time.
  --time_partitioning_type <value>
                           Enables time-based partitioning on a table and sets the partition type. Currently, the only possible value is DAY which generates one partition per day.
  --require_partition_filter <value>
                           If specified, a partition filter is required for queries over the supplied table. This flag can only be used with a partitioned table.
  --use_avro_logical_types <value>
                           If sourceFormat is set to AVRO, indicates whether to convert logical types into their corresponding types (such as TIMESTAMP) instead of only using their raw types (such as INTEGER).
  --dataset_id <value>     The default dataset to use for requests. This flag is ignored when not applicable. You can set the value to [PROJECT_ID]:[DATASET] or [DATASET]. If [PROJECT_ID] is missing, the default project is used. You can override this setting by specifying the --project_id flag. The default value is ''.
  --debug_mode <value>     Set logging level to debug. The default value is false.
  --job_id <value>         The unique job ID to use for the request. If not specified in a job creation request, a job ID is generated. This flag applies only to commands that create jobs: cp, extract, load, and query.
  --location <value>       A string corresponding to your region or multi-region location.
  --project_id <value>     The project ID to use for requests. The default value is ''.
  --synchronous_mode <value>
                           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
  --sync <value>           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
```

### mk

```
mk 1.0
Usage: mk [options] tablespec schema

  --help                   prints this usage text
  tablespec                [PROJECT_ID]:[DATASET].[TABLE]
  schema                   The path to a local JSON schema file or a comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]. The default value is ''.
  --clustering_fields <value>
                           A comma-separated list of column names used to cluster a table. This flag is currently available only for partitioned tables. When specified, the table is partitioned and then clustered using these columns.
  -d, --dataset <value>    When specified, creates a dataset. The default value is false.
  --default_partition_expiration <value>
                           An integer that specifies the default expiration time, in seconds, for all partitions in newly-created partitioned tables in the dataset. A partition's expiration time is set to the partition's UTC date plus the integer value. If this property is set, it overrides the dataset-level default table expiration if it exists. If you supply the --time_partitioning_expiration flag when you create or update a partitioned table, the table-level partition expiration takes precedence over the dataset-level default partition expiration.
  --default_table_expiration <value>
                           An integer that specifies the default lifetime, in seconds, for newly-created tables in a dataset. The expiration time is set to the current UTC time plus this value.
  --description <value>    The description of the dataset or table.
  --destination_kms_key <value>
                           The Cloud KMS key used to encrypt the table data.
  --display_name <value>   The display name for the transfer configuration. The default value is ''.
  --expiration <value>     An integer that specifies the table or view's lifetime in milliseconds. The expiration time is set to the current UTC time plus this value.
  --external_table_definition <value>
                           Specifies a table definition to used to create an external table. The value can be either an inline table definition or a path to a file containing a JSON table definition. The format of an inline definition is schema@format=uri.
  -f, --force <value>      When specified, if a resource already exists, the exit code is 0. The default value is false.
  --label <value>          A label to set on the table. The format is [KEY]:[VALUE]. Repeat this flag to specify multiple labels.
  --require_partition_filter <value>
                           When specified, this flag determines whether to require a partition filter for queries over the supplied table. This flag only applies to partitioned tables. The default value is true.
  --schema <value>         The path to a local JSON schema file or a comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]. The default value is ''.
  -t, --table <value>      When specified, create a table. The default value is false.
  --time_partitioning_expiration <value>
                           An integer that specifies (in seconds) when a time-based partition should be deleted. The expiration time evaluates to the partition's UTC date plus the integer value. A negative number indicates no expiration.
  --time_partitioning_field <value>
                           The field used to determine how to create a time-based partition. If time-based partitioning is enabled without this value, the table is partitioned based on the load time.
  --time_partitioning_type <value>
                           Enables time-based partitioning on a table and sets the partition type. Currently, the only possible value is DAY which generates one partition per day.
  --use_legacy_sql <value>
                           When set to false, uses a standard SQL query to create a view. The default value is false (uses Standard SQL).
  --view <value>           When specified, creates a view. The default value is false.
  --dataset_id <value>     The default dataset to use for requests. This flag is ignored when not applicable. You can set the value to [PROJECT_ID]:[DATASET] or [DATASET]. If [PROJECT_ID] is missing, the default project is used. You can override this setting by specifying the --project_id flag. The default value is ''.
  --debug_mode <value>     Set logging level to debug. The default value is false.
  --job_id <value>         The unique job ID to use for the request. If not specified in a job creation request, a job ID is generated. This flag applies only to commands that create jobs: cp, extract, load, and query.
  --location <value>       A string corresponding to your region or multi-region location.
  --project_id <value>     The project ID to use for requests. The default value is ''.
  --synchronous_mode <value>
                           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
  --sync <value>           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
```

### query

```
query
Usage: query [options]

  --help                   prints this usage text
  --append_table <value>   When specified, append data to a destination table. The default value is false.
  --batch <value>          When specified, run the query in batch mode. The default value is false.
  --clustering_fields <value>
                           If specified, a comma-separated list of columns is used to cluster the destination table in a query. This flag must be used with the time partitioning flags to create either an ingestion-time partitioned table or a table partitioned on a DATE or TIMESTAMP column. When specified, the table is first partitioned, and then it is clustered using the supplied columns.
  --destinationKmsKey <value>
                           The Cloud KMS key used to encrypt the destination table data.
  --destinationSchema <value>
                           The path to a local JSON schema file or a comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]. The default value is ''.
  --destinationTable <value>
                           The name of the destination table for writing query results. The default value is ''
  --dryRun <value>         When specified, the query is validated but not run.
  --externalTableDefinition <value>
                           The table name and schema definition used in an external table query. The schema can be a path to a local JSON schema file or a comma-separated list of column definitions in the form [FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]. The format for supplying the table name and schema is: [TABLE]::[PATH_TO_FILE] or [TABLE]::[SCHEMA]@[SOURCE_FORMAT]=[CLOUD_STORAGE_URI]. Repeat this flag to query multiple tables.
  --label <value>          A label to apply to a query job in the form [KEY]:[VALUE]. Repeat this flag to specify multiple labels.
  --maximum_bytes_billed <value>
                           An integer that limits the bytes billed for the query. If the query goes beyond the limit, it fails (without incurring a charge). If not specified, the bytes billed is set to the project default.
  --parameters <value>     comma-separated query parameters in the form [NAME]:[TYPE]:[VALUE]. An empty name creates a positional parameter. [TYPE] may be omitted to assume a STRING value in the form: name::value or ::value. NULL produces a null value.
  --replace <value>        If specified, overwrite the destination table with the query results. The default value is false.
  --require_cache <value>  If specified, run the query only if results can be retrieved from the cache.
  --require_partition_filter <value>
                           If specified, a partition filter is required for queries over the supplied table. This flag can only be used with a partitioned table.
  --schema_update_option <value>
                           When appending data to a table (in a load job or a query job), or when overwriting a table partition, specifies how to update the schema of the destination table. Possible values include:

 ALLOW_FIELD_ADDITION: Allow
new fields to be added
 ALLOW_FIELD_RELAXATION: Allow relaxing REQUIRED fields to NULLABLE
  --time_partitioning_expiration <value>
                           An integer that specifies (in seconds) when a time-based partition should be deleted. The expiration time evaluates to the partition's UTC date plus the integer value. A negative number indicates no expiration.
  --time_partitioning_field <value>
                           The field used to determine how to create a time-based partition. If time-based partitioning is enabled without this value, the table is partitioned based on the load time.
  --time_partitioning_type <value>
                           Enables time-based partitioning on a table and sets the partition type. Currently, the only possible value is DAY which generates one partition per day.
  --use_cache <value>      When specified, caches the query results. The default value is true.
  --use_legacy_sql <value>
                           When set to false, runs a standard SQL query. The default value is false (uses Standard SQL).
  --dataset_id <value>     The default dataset to use for requests. This flag is ignored when not applicable. You can set the value to [PROJECT_ID]:[DATASET] or [DATASET]. If [PROJECT_ID] is missing, the default project is used. You can override this setting by specifying the --project_id flag. The default value is ''.
  --debug_mode <value>     Set logging level to debug. The default value is false.
  --job_id <value>         The unique job ID to use for the request. If not specified in a job creation request, a job ID is generated. This flag applies only to commands that create jobs: cp, extract, load, and query.
  --location <value>       A string corresponding to your region or multi-region location.
  --project_id <value>     The project ID to use for requests. The default value is ''.
  --synchronous_mode <value>
                           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
  --sync <value>           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
```

### rm

```
rm
Usage: rm [options] tablespec

  --help                   prints this usage text
  tablespec                [PROJECT_ID]:[DATASET].[TABLE]
  -d, --dataset <value>    When specified, deletes a dataset. The default value is false.
  -f, --force <value>      When specified, deletes a table, view, model, or dataset without prompting. The default value is false.
  -m, --model <value>      When specified, deletes a BigQuery ML model.
  -r, --recursive <value>  When specified, deletes a dataset and any tables, table data, or models in it. The default value is false.
  -t, --table <value>      When specified, deletes a table. The default value is false.
  --dataset_id <value>     The default dataset to use for requests. This flag is ignored when not applicable. You can set the value to [PROJECT_ID]:[DATASET] or [DATASET]. If [PROJECT_ID] is missing, the default project is used. You can override this setting by specifying the --project_id flag. The default value is ''.
  --debug_mode <value>     Set logging level to debug. The default value is false.
  --job_id <value>         The unique job ID to use for the request. If not specified in a job creation request, a job ID is generated. This flag applies only to commands that create jobs: cp, extract, load, and query.
  --location <value>       A string corresponding to your region or multi-region location.
  --project_id <value>     The project ID to use for requests. The default value is ''.
  --synchronous_mode <value>
                           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
  --sync <value>           If set to true, wait for the command to complete before returning, and use the job completion status as the error code. If set to false, the job is created, and successful completion status is used for the error code. The default value is true.
```

## Disclaimer

This is not an official Google product.
