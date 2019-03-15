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

import java.io._
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.PrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Collections

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http._
import com.google.api.client.util.{PemReader, SecurityUtils}
import com.google.cloud.gszutil.GSXMLModel.ListBucketResult
import com.google.common.io.ByteSource


object GSXML {
  val StorageScope: java.util.Collection[String] = Collections.singleton("https://www.googleapis.com/auth/devstorage.read_write")
  val StorageEndpoint = "https://storage.googleapis.com/"
  val TokenURI = "https://oauth2.googleapis.com/token"
  val ContentType = "application/octet-stream"

  trait CredentialProvider {
    def getCredential: GoogleCredential
  }

  object DefaultCredentialProvider extends CredentialProvider {
    override def getCredential: GoogleCredential =
      GoogleCredential.getApplicationDefault.createScoped(StorageScope)
  }

  case class PrivateKeyCredentialProvider(privateKeyId: String, privateKeyPem: String, serviceAccountId: String) extends CredentialProvider {
    def getCredential: GoogleCredential = {
      GoogleCredential.getApplicationDefault().toBuilder
        .setServiceAccountId(serviceAccountId)
        .setServiceAccountPrivateKeyId(privateKeyId)
        .setServiceAccountPrivateKey(keyFromPem(privateKeyPem))
        .setServiceAccountScopes(StorageScope)
        .setTokenServerEncodedUrl(TokenURI)
        .build()
    }
  }

  class HttpResponseByteSource(response: HttpResponse) extends ByteSource {
    override def openStream(): InputStream = response.getContent
  }

  @transient
  private var client: XMLStorage = _

  def getClient(credential: CredentialProvider = DefaultCredentialProvider, storageEndpoint: String = StorageEndpoint): XMLStorage = {
    if (client == null) {
      client = XMLStorage(credential, storageEndpoint)
      client
    } else client
  }

  class ListBucketResultIterator(gcs: XMLStorage, bucket: String, marker: String = "", maxKeys: Int = 1000, prefix: String = "", delimiter: String = "/") extends Iterator[ListBucketResult] {
    private val markers: collection.mutable.Stack[String] = collection.mutable.Stack[String](marker)
    private val stack: collection.mutable.Stack[ListBucketResult] = new collection.mutable.Stack[ListBucketResult]()

    private def getNext(m: String): GSXMLModel.ListBucketResult = {
      val req = gcs.listBucket(bucket, m, maxKeys, prefix, delimiter)
      val resp = req.execute()
      val xml = readString(resp)
      val result = gcs.xmlMapper.readValue(xml, classOf[GSXMLModel.ListBucketResult])
      stack.push(result)
      result
    }

    private def saveNext(): Unit = {
      if (markers.nonEmpty) {
        val next = getNext(markers.pop())
        if (next.IsTruncated) {
          markers.push(next.NextMarker)
        }
      }
    }

    override def hasNext: Boolean = {
      if (stack.nonEmpty) {
        true
      } else {
        saveNext()
        stack.nonEmpty || markers.nonEmpty
      }
    }

    override def next(): ListBucketResult = {
      if (stack.nonEmpty) {
        stack.pop()
      } else {
        saveNext()
        stack.pop()
      }
    }
  }

  case class XMLStorage(credential: CredentialProvider, endpoint: String) {
    @transient private val requestFactory = GoogleNetHttpTransport.newTrustedTransport.createRequestFactory(credential.getCredential)
    @transient val xmlMapper: XmlMapper = {
      val mapper = new XmlMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper
    }

    def listBucket(bucket: String, marker: String = "", maxKeys: Int = 1000, prefix: String = "", delimiter: String = "/"): HttpRequest = {
      val params = s"?prefix=${URLEncoder.encode(prefix, "UTF-8")}&marker=${URLEncoder.encode(marker, "UTF-8")}&max-keys=$maxKeys&delimiter=${URLEncoder.encode(delimiter, "UTF-8")}"
      val uri = endpoint + URLEncoder.encode(bucket, "UTF-8") + params
      requestFactory.buildGetRequest(new GenericUrl(uri))
    }

    def listBucketRecursive(bucket: String, marker: String = "", maxKeys: Int = 1000, prefix: String = "", delimiter: String = "/"): Iterator[ListBucketResult] =
      new ListBucketResultIterator(this, bucket, marker, maxKeys, prefix, delimiter)

    def putObject(bucket: String, key: String, inputStream: InputStream, contentType: String = ContentType): HttpRequest = {
      val uri = endpoint + URLEncoder.encode(bucket + "/" + key, "UTF-8")
      val content = new InputStreamContent(contentType, inputStream)
      requestFactory.buildPutRequest(new GenericUrl(uri), content)
    }
  }

  def readString(response: HttpResponse): String =
    new HttpResponseByteSource(response)
      .asCharSource(StandardCharsets.UTF_8)
      .read()

  def keyFromPem(pem: String): PrivateKey = {
    val pemBytes = PemReader.readFirstSectionAndClose(new StringReader(pem), "PRIVATE KEY")
      .getBase64DecodedBytes
    val keySpec = new PKCS8EncodedKeySpec(pemBytes)
    SecurityUtils.getRsaKeyFactory.generatePrivate(keySpec)
  }
}
