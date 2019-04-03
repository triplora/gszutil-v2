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
4. Copy the assembly jar to the z/OS library `<HLQ>.JZOS.LOADLIB` specified in [GSZUTIL.PRC](GSZUTIL.PRC)
5. Convert PROC and JCL files (`GSZUTIL.PRC`, `GSZUTIL.JCL`) to EBCDIC with `iconv -t EBCDICUS -f UTF-8`
6. Copy PROC and JCL files to z/OS with `sftp`
6. edit `GSZUTIL.PRC` and set source DSN and destination bucket
6. submit `GSZUTIL.JCL` as an MVS job


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
