package org.apache.spark.sql.execution.datasources.zfile

import java.net.URI

import com.google.cloud.gszutil.Decoding.CopyBook
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._

class ZFileFormat extends FileFormat with DataSourceRegister with Serializable {
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
    val broadcastedCopyBook = sparkSession.sparkContext
      .broadcast(CopyBook(options("copybook")))

    file: PartitionedFile => {
      val copyBook = broadcastedCopyBook.value
      val filePath = new Path(new URI(file.filePath))
      if (requiredSchema.length == 0) {
        Iterator.empty
      } else {
        assert(requiredSchema == copyBook.getSchema)
        copyBook.reader.readDDInternal(filePath.getName)
      }
    }
  }
}
