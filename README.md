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
  -p, --parallelism <value>
                           number of concurrent writers (default: 5)
```


## Disclaimer

This is not an official Google product.
