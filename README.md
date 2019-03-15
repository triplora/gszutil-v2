# GSZUtil

GSZUtil uploads data to Google Cloud Storage from a z/OS batch job.

(Experimental)


## Pre-Requisites

* [IBM SDK for z/OS, Java Technology Edition, Version 8](https://developer.ibm.com/javasdk/support/zos/)
* [SBT](https://www.scala-sbt.org/download.html)


## Installation

1. Extract IBM JDK to `/opt/zjdk` or similar
2. Modify [build.sbt](build.sbt) with the path to `jzos.jar`
3. Modify [Credentials](src/main/scala/com/google/cloud/gszutil/Credentials.scala) and add your service account email and private key PEM copied from JSON keyfile.
3. Run `sbt assembly` from the repository root to build an assembly jar
4. Copy the assembly jar to the z/OS library `<HLQ>.JZOS.LOADLIB` specified in [GSZUTIL.PRC](GSZUTIL.PRC)
5. Convert PROC and JCL files (`GSZUTIL.PRC`, `GSZUTIL.JCL`) to EBCDIC with `iconv -t EBCDICUS -f UTF-8`
6. Copy PROC and JCL files to z/OS with `sftp`
6. edit `GSZUTIL.PRC` and set source DSN and destination bucket
6. submit `GSZUTIL.JCL` as an MVS job


## Encoding Conversion

The examples below demonstrate conversion between EBCDIC and UTF-8.


### Convert EBCDIC to UTF-8

```sh
iconv -f EBCDICUS -t UTF-8 GSZUTILPRC | tr '\205' '\n' | tr -d '\302' | tr -cd '\11\12\15\40-\176' > GSZUTIL.PRC
iconv -f EBCDICUS -t UTF-8 GSZUTILJCL | tr '\205' '\n' | tr -d '\302' | tr -cd '\11\12\15\40-\176' > GSZUTIL.JCL
```

### Convert UTF-8 to EBCDIC

```sh
iconv -t EBCDICUS -f UTF-8 GSZUTIL.PRC > GSZUTILPRC
```

## Disclaimer

This is not an official Google product.
