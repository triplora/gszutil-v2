package com.google.cloud.gszutil.io.`export`

import java.nio.ByteBuffer

import com.google.cloud.bigquery._
import com.google.cloud.bqsh.cmd.Result
import com.google.cloud.gszutil.BinaryEncoder
import com.google.cloud.gszutil.Encoding.StringToBinaryEncoder
import com.google.cloud.imf.gzos.Ebcdic
import com.google.cloud.imf.util.CloudLogging

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class LocalFileExporter extends FileExporter {
  protected var export: FileExport = _
  override def isOpen: Boolean = export != null
  override def currentExport: FileExport = export
  override def newExport(e: FileExport): Unit = export = e
  override def endIfOpen: Unit = if (export != null) export.close()

  override def exportBQSelectResult(rows: java.lang.Iterable[FieldValueList],
                                    bqSchema: FieldList,
                                    mvsEncoders: Array[BinaryEncoder]): Result = {
    if (!isOpen) {
      // export is closed, just not export
      return Result.Success
    }
    val nCols = bqSchema.size()
    val nEnc = mvsEncoders.length
    val bqFields = (0 until nCols).map{i => bqSchema.get(i)}

    val tbl = (0 until math.max(nCols, nEnc)).map{i =>
      val f = bqFields.lift(i).map{x =>
        s"${x.getName}\t${x.getType.getStandardType}"
      }.getOrElse("\t")
      val e = mvsEncoders.lift(i).map{x =>s"${x.bqSupportedType}\t$x"}.getOrElse("\t")
      s"$i\t$f\t$e"
    }.mkString("\n")
    CloudLogging.stdout(s"LocalFileExporter Starting BigQuery Export:\n" +
      "Field Name\tBQ Type\tEnc Type\tEncoder\n$tbl")

    val bqTypes: Array[StandardSQLTypeName] =
      (0 until nCols).map(i => bqSchema.get(i).getType.getStandardType).toArray
    val encTypes: Array[StandardSQLTypeName] =
      mvsEncoders.map(_.bqSupportedType)

    // validate field count
    if (nCols != nEnc) {
      val msg = s"InterpreterContext ERROR Export schema mismatch:\nBigQuery field count $nCols " +
        s"!= $nEnc encoders"
      CloudLogging.stdout(msg)
      CloudLogging.stderr(msg)
      return Result.Failure(msg)
    }

    // validate field types
    // fail if target type is not string and source type does not match target type
    (0 until nCols)
      .filter{i => encTypes(i) != StandardSQLTypeName.STRING && bqTypes(i) != encTypes(i)} match {
      case x if x.nonEmpty =>
        val fields =
          x.map(i => s"$i\t${bqFields(i).getName}\t${bqTypes(i)}\t${encTypes(i)}").mkString("\n")
        val msg = s"LocalFileExporter ERROR Export field type mismatch:\n$fields"
        CloudLogging.stdout(msg)
        CloudLogging.stderr(msg)
        return Result.Failure(msg)
      case _ =>
    }

    val buf = ByteBuffer.allocate(export.lRecl)
    rows.forEach{row =>
      buf.clear()
      MVSBinaryRowEncoder.toBinary(row, mvsEncoders, buf) match {
        case result if result.exitCode == 0 =>
          export.appendBytes(buf.array())
        case err =>
          // Print helpful error message containing schema, row, encoder
          val sb = new StringBuilder
          sb.append("Export failed to encode row:\n")
          sb.append("Field Name\tBQ Type\tEncoder\tValue\n")
          (0 until math.max(bqFields.length,row.size())).foreach{i =>
            val field = bqFields.lift(i)
            val name = field.map(_.getName).getOrElse("None")
            val bqType = field.map(_.getType.getStandardType.toString).getOrElse("None")
            val encoder = mvsEncoders.lift(i).map(_.toString).getOrElse("None")
//            val value = Option(row.get(i).getValue).getOrElse("null")
//            sb.append(s"$name\t$bqType\t$encoder\t$value\n")
          }
          val msg = sb.result()
          CloudLogging.stdout(msg)
          CloudLogging.stderr(msg)
          return err
      }
    }

    Result.Success
  }

  override def exportPipeDelimitedRows(rows: java.lang.Iterable[FieldValueList]): Result = {
    def run(): Unit = {
      rows.forEach{
        row =>
          val lineBuf = ListBuffer.empty[String]
          (0 until row.size()).foreach{
            i =>
              lineBuf.append(row.get(i).getStringValue)
          }
          val line = lineBuf.toSeq.mkString("|")
          val lineToExport = if (line.length < export.lRecl) {
            val rpad = export.lRecl - line.length
            line + (" " * rpad)
          } else line

          val bytes = StringToBinaryEncoder(Ebcdic, export.lRecl)
            .encode(lineToExport.substring(0, export.lRecl-1))

          export.appendBytes(bytes)
      }
    }

    Try(run) match {
      case Success(_) => Result.Success
      case Failure(t) => Result.Failure(t.getMessage)
    }
  }
}
