package com.google.cloud.pso

import java.nio.ByteBuffer

import com.google.cloud.gszutil.Decoding._
import com.google.cloud.gszutil.ZReader

case class SkuDlyPosRec(
                         STORE_NBR: Short,
                         ITEM_NBR: Int,
                         WM_YR_WK: Short,
                         REPORT_CODE: Short,
                         SELL_PRICE: BigDecimal,
                         SALES: BigDecimal,
                         QTY: Int,
                         SAT_QTY: Int,
                         SUN_QTY: Int,
                         MON_QTY: Int,
                         TUE_QTY: Int,
                         WED_QTY: Int,
                         THU_QTY: Int,
                         FRI_QTY: Int,
                         DBC_LOCATION: String,
                         PROCESS_TYPE_FLAG: String
                       )

object SkuDlyPosRec {
  def apply(dd: String): Iterator[SkuDlyPosRec] = new Reader().readDD(dd)

  val ColumnNames: Seq[String] = Array(
    "STORE_NBR","ITEM_NBR","WM_YR_WK","REPORT_CODE","SELL_PRICE","SALES","QTY","SAT_QTY","SUN_QTY","MON_QTY","TUE_QTY","WED_QTY","THU_QTY","FRI_QTY","DBC_LOCATION","PROCESS_TYPE_FLAG"
  )

  class Reader extends DataSet[SkuDlyPosRec] {
    override val LRECL = 56
    private val buf = ByteBuffer.allocate(LRECL)

    private val int = new IntDecoder4
    private val short = new IntDecoder2
    private val char = new CharDecoder
    private val float = new NumericDecoder

    override def readDD(ddName: String): Iterator[SkuDlyPosRec] =
      read(ZReader.readRecords(ddName))

    override def read(array: Array[Byte]): SkuDlyPosRec = {
      buf.clear()
      buf.put(array)
      buf.flip()
      read(buf)
    }

    override def read(records: Iterator[Array[Byte]]): Iterator[SkuDlyPosRec] =
      records.takeWhile(_.length == LRECL).map(read)

    override def read(buf: ByteBuffer): SkuDlyPosRec =
      SkuDlyPosRec(
        STORE_NBR = short(buf),
        ITEM_NBR = int(buf),
        WM_YR_WK = short(buf),
        REPORT_CODE = short(buf),
        SELL_PRICE = float(buf),
        SALES = float(buf),
        QTY = int(buf),
        SAT_QTY = int(buf),
        SUN_QTY = int(buf),
        MON_QTY = int(buf),
        TUE_QTY = int(buf),
        WED_QTY = int(buf),
        THU_QTY = int(buf),
        FRI_QTY = int(buf),
        DBC_LOCATION = char(buf),
        PROCESS_TYPE_FLAG = char(buf)
      )
  }
}
