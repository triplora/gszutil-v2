package com.google.cloud.imf.grecv

import com.google.cloud.imf.gzos.Util
import com.google.cloud.imf.gzos.pb.GRecvProto.Record.Field
import com.google.cloud.imf.gzos.pb.GRecvProto.{GRecvRequest, Record}
import com.google.cloud.imf.util.{Logging, SecurityUtils}
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec

trait TCPIPSpec extends AnyFlatSpec with Logging {
  val Host = "127.0.0.1"
  val PlaintextPort = 51771
  val Port = 51770
  Util.configureLogging(true)
  SecurityUtils.useConscrypt()

  val lrecl = 100
  val blksz = 30000
  val len = blksz * 100
  val r = Record.newBuilder
  val zos = Util.zProvider
  zos.init()

  r.addFieldBuilder().setName("a").setTyp(Field.FieldType.STRING).setSize(20)
  r.addFieldBuilder().setName("b").setTyp(Field.FieldType.STRING).setSize(20)
  r.addFieldBuilder().setName("c").setTyp(Field.FieldType.STRING).setSize(20)
  r.addFieldBuilder().setName("d").setTyp(Field.FieldType.STRING).setSize(20)
  r.addFieldBuilder().setName("e").setTyp(Field.FieldType.STRING).setSize(20)

  val keypair = zos.getKeyPair()

  val request = GRecvRequest.newBuilder
    .setSchema(r.build)
    .setLrecl(lrecl)
    .setBlksz(blksz)
    .setPrincipal(zos.getPrincipal())
    .setPublicKey(ByteString.copyFrom(keypair.getPublic.getEncoded))
    .setBasepath("gs://gszutil-test/v4/grpc")

  val signature = ByteString.copyFrom(
    SecurityUtils.sign(keypair.getPrivate, request.clearSignature().build().toByteArray))

  request.setSignature(signature)

  val in = Util.random(len, lrecl, blksz)
}
