package com.google.cloud.gszutil.io

import java.nio.channels.ReadableByteChannel

case class DDChannel(rc: ReadableByteChannel, lRecl: Int, blkSize: Int)
