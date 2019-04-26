package com.google.cloud.gszutil

import java.net.InetAddress
import java.sql.Date
import java.time.LocalDate

import com.google.cloud.gszutil.GSXML.CredentialProvider
import org.apache.spark.sql.SparkSession

import scala.util.Random

object OrcWriter {
  def generateRecord(r: Random): (Date, Int, String, Double) = {
    (Date.valueOf(LocalDate.now()), r.nextInt(1000), r.nextString(8), r.nextDouble)
  }

  def run(config: Config, cp: CredentialProvider): Unit = {
    DNSCache("www.googleapis.com",
      Seq("199.36.153.4","199.36.153.5","199.36.153.6","199.36.153.7"))

    val resolved: Set[String] = InetAddress.getAllByName("www.googleapis.com")
      .map(_.getHostAddress).toSet
    val expected: Set[String] = Set("199.36.153.4","199.36.153.5","199.36.153.6","199.36.153.7")

    if (resolved != expected) {
      System.err.println(s"Unexpected IPs resolved for www.googleapis.com: $resolved")
    }

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("GSZUtil")
      .getOrCreate()

    import spark.implicits._

    val r = new Random()

    val data = (0 until 10).map{_ =>
      generateRecord(r)
    }

    val rdd = spark.sparkContext.makeRDD(data, 1)
    val df = rdd.toDF("date", "id", "description", "value")

    val dest = s"gs://${config.bq.bucket}/${config.bq.prefix}"
    df.write.orc(dest+".orc")
    df.write.parquet(dest+".parquet")
  }
}
