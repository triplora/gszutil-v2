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
git clone $REPO_URL
cd gszutil
./install_ibm_libs /opt/J8.0_64

# Build Dependency Jar
sbt assemblyPackageDependency

# Build Application Jar
sbt package

ls -ldh target/scala-2.11/gszutil.dep.jar
ls -ldh target/scala-2.11/gszutil-*.jar
