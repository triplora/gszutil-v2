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

import java.net.InetAddress

import sun.net.util.IPAddressUtil

object DNSCache {
  private def getField[T](fieldName: String, c: Class[T], instance: java.lang.Object = null): Object = {
    val field = c.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(instance)
  }

  def apply(hostname: String, ips: Seq[String] = Seq.empty, expiration: Long = -1): Unit = {
    val host = hostname.toLowerCase
    val addresses: Array[InetAddress] = ips.map{ip =>
      val ipBytes = IPAddressUtil.textToNumericFormatV4(ip)
      InetAddress.getByAddress(host, ipBytes)
    }.toArray

    // InetAddress.CacheEntry
    val cacheEntryClass = Class.forName("java.net.InetAddress$CacheEntry")
    val cacheEntryConstructor = cacheEntryClass.getDeclaredConstructors.head
    cacheEntryConstructor.setAccessible(true)
    val cacheEntry = cacheEntryConstructor.newInstance(addresses, java.lang.Long.valueOf(expiration)).asInstanceOf[java.lang.Object]

    // InetAddress.Cache
    val cacheClass: Class[_] = Class.forName("java.net.InetAddress$Cache")

    val positiveCache = getField("cache", cacheClass, getField("addressCache", classOf[InetAddress]))
      .asInstanceOf[java.util.Map[String,Any]]

    val negativeCache = getField("cache", cacheClass, getField("negativeCache", classOf[InetAddress]))
      .asInstanceOf[java.util.Map[String,Any]]

    // Update positive cache
    if (ips.isEmpty)
      positiveCache.remove(host)
    else
      positiveCache.put(host, cacheEntry)

    // Remove host from negative cache
    negativeCache.remove(host)
  }
}
