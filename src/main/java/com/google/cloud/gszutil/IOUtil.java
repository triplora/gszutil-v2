package com.google.cloud.gszutil;

import com.google.cloud.gszutil.io.ZRecordReaderT;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class IOUtil {
    private static Logger logger = LogManager.getLogger(IOUtil.class);
    private static long yieldCount = 0;
    public static long getYieldCount() {
        return yieldCount;
    }
    public static void reset() {
        yieldCount = 0;
    }
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
}
