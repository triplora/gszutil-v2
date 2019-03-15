# GSZUtil

GSZUtil uploads data to Google Cloud Storage from a z/OS batch job.

(Experimental)


## Pre-Requisites

[IBM SDK for z/OS, Java Technology Edition, Version 8](https://developer.ibm.com/javasdk/support/zos/)
[SBT](https://www.scala-sbt.org/download.html)


## Installation

1. Install the IBM JDK to `/opt/zjdk`
2. `cd` to the repository root
3. modify [Credentials](src/main/scala/com/google/cloud/gszutil/Credentials.scala) and add your service account json key contents.
3. run `sbt assembly` to build an assembly jar
4. `sftp` the assembly jar to z/OS `<HLQ>.JZOS.LOADLIB` in [JVMPRC86](JVMPRC86.txt)
5. convert PROC and JCL files (`JVMPRC86`, `JVMJCL86`) to EBCDIC and `sftp` to z/OS
6. edit `JVMPRC86` and set source DSN and destination bucket
6. submit `JVMJCL86` as an MVS job


## Encoding Conversion

The examples below demonstrate conversion between EBCDIC and UTF-8.


## EBCDIC to UTF-8

```sh
iconv -f EBCDICUS -t UTF-8 JVMJCL86 | tr '\205' '\n' | tr -d '\302' | tr -cd '\11\12\15\40-\176' > JVMJCL86.txt
```

## UTF-8 to EBCDIC

```sh
iconv -t EBCDICUS -f UTF-8 JVMJCL86.txt > JVMJCL86
```

## Disclaimer

This is not an official Google product.
