package org.apache.spark.sql.execution.datasources.zfile

import java.net.URI

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.{Util, ZOS}
import com.google.cloud.gszutil.Util.{DebugLogging, Logging}
import com.google.cloud.gszutil.ZReader.ZIterator
import com.google.cloud.gszutil.io.ByteArrayRecordReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._

class ZFileFormat extends FileFormat with DataSourceRegister with Serializable  with DebugLogging {

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] =
    options.get("copybook").map(CopyBook(_).getSchema)

  override def toString: String = "ZFILE"

  override def shortName(): String = "zfile"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ZFileFormat]

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = false

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = throw new NotImplementedError()

  override def supportDataType(dataType: DataType, isReadPath: Boolean): Boolean = dataType match {
    case _: DecimalType => true
    case _: StringType => true
    case _: LongType => true
    case _: IntegerType => true
    case _ => false
  }

  override def vectorTypes(requiredSchema: StructType, partitionSchema: StructType, sqlConf: SQLConf): Option[Seq[String]] = None

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = false

  override protected def buildReader(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    logger.info(s"building reading reader for $requiredSchema with options $options")
    val broadcastedCopyBook = sparkSession.sparkContext
      .broadcast(CopyBook(options("copybook")))

    file: PartitionedFile => {
      val copyBook = broadcastedCopyBook.value
      val schema = copyBook.getSchema
      val filePath = new Path(new URI(file.filePath))
      System.out.println(s"reading from ${file.filePath} $filePath")
      if (requiredSchema.length == 0 || requiredSchema != schema) {
        throw new RuntimeException("asdf")
        System.out.println(s"required schema\n$requiredSchema\n\n$schema")
        Iterator.empty
      } else {
        System.out.println("asdf123")
        copyBook.reader.readA(new ZIterator(new ByteArrayRecordReader(Util.readB("imsku.bin"), copyBook.lRecl, copyBook.lRecl * 10)))
        throw new RuntimeException("asdf")

        //Iterator.empty
        //copyBook.reader.readA(filePath.getName)
      }
    }
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    logger.info(s"building reading reader for $requiredSchema with options $options")
    val broadcastedCopyBook = sparkSession.sparkContext
      .broadcast(CopyBook(options("copybook")))

    file: PartitionedFile => {
      val copyBook = broadcastedCopyBook.value
      val schema = copyBook.getSchema
      val filePath = new Path(new URI(file.filePath))
      System.out.println(s"reading from ${filePath.getName}")
      val schemaDiff = Util.schemaDiff(requiredSchema, schema)
      if (requiredSchema.length == 0 || schemaDiff.nonEmpty) {
        throw new RuntimeException(s"schema mismatch\n${schemaDiff.mkString("\n")}")
      } else {
        System.out.println("reading from imsku.bin")
        if (System.getProperty("java.vm.vendor").contains("IBM")) {
          copyBook.reader.readA(ZIterator(ZOS.readDD(filePath.getName)))
        } else {
          copyBook.reader.readA(ZIterator(new ByteArrayRecordReader(Util.readB("imsku.bin"), copyBook.lRecl, copyBook.lRecl * 10)))
        }

        //Iterator.empty
        //copyBook.reader.readA(filePath.getName)
      }
    }
  }
}
