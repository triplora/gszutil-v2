package org.apache.spark.sql.execution.datasources.zfile

import java.net.URI

import com.google.cloud.gszutil.Decoding.CopyBook
import com.google.cloud.gszutil.{Util, ZOS}
import com.google.cloud.gszutil.Util.DebugLogging
import com.google.cloud.gszutil.io.{ByteArrayRecordReader, ZIterator}
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
      logger.info(s"reading from ${file.filePath} $filePath")
      if (requiredSchema.length == 0 || requiredSchema != schema) {
        Iterator.empty
      } else {
        //val (data,offset) = ZIterator(new ByteArrayRecordReader(Util.readB("imsku.bin"), copyBook.lRecl, copyBook.lRecl * 10))
        //copyBook.reader.readA(data, offset)

        copyBook.reader.readA(filePath.getName)
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
        if (System.getProperty("java.vm.vendor").contains("IBM")) {
          val (data,offset) = ZIterator(ZOS.readDD(filePath.getName))
          copyBook.reader.readA(data,offset)
        } else {
          //TODO remove after testing
          val (data,offset) = ZIterator(new ByteArrayRecordReader(Util.readB("imsku.bin"), copyBook.lRecl, copyBook.lRecl * 10))
          copyBook.reader.readA(data,offset)
        }
      }
    }
  }
}
