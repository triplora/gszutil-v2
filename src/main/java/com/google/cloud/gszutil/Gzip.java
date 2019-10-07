package com.google.cloud.gszutil;

import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class Gzip {
    public static byte[] decompress(byte[] buf, int offset, int length) throws IOException {
        return ByteStreams.toByteArray(new GZIPInputStream(new ByteArrayInputStream(buf, offset,
                length)));
    }
}
