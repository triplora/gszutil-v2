package com.google.cloud.gszutil;

import com.google.cloud.gszutil.io.ZRecordReaderT;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.zip.Deflater;

public class IOUtil {
    private static Logger logger = LogManager.getLogger(IOUtil.class);
    private static long yieldCount = 0;
    public static long getYieldCount() { return yieldCount; }
    public static void reset() { yieldCount = 0; }
    public static int readBlock(ZRecordReaderT r, byte[] buf) {
        int bytesRead = r.lRecl();
        int lrecl = r.lRecl();
        int i = 0;
        final int n = buf.length;
        while (bytesRead > 0 && i < n){
            bytesRead = r.read(buf, i, lrecl);
            i += bytesRead;
        }

        if (bytesRead == 0){
            Thread.yield();
            yieldCount += 1;
        }

        if (i == -1) return -1;
        else if (bytesRead == -1) return i+1;
        else return i;
    }

    public static int compressAndSend(byte[] data,
                                      int bytesRead,
                                      byte[] deflateBuf,
                                      Deflater deflater,
                                      ZMQ.Socket socket) {
        // Compress
        deflater.setInput(data, 0, bytesRead);
        deflater.finish();
        int compressed = deflater.deflate(deflateBuf,0,deflateBuf.length,Deflater.FULL_FLUSH);
        deflater.reset();

        // Send
        if (socket.send(deflateBuf, 0, compressed, 0))
            return compressed;
        else return -1;
    }
}
