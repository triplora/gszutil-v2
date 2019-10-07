package java.io;

import com.google.common.io.ByteStreams;

import java.util.zip.ResettableGzipInputStream;

/** Created in java.io package to access protected fields */
public class Gzip extends ByteArrayInputStream {
    private final ResettableGzipInputStream gzis;
    public Gzip() throws IOException {
        this(new byte[0]);
    }

    public Gzip(byte[] buf) throws IOException {
        super(buf);
        gzis = new ResettableGzipInputStream(this, 32*1024);
    }

    public Gzip fromBytes(byte[] buf) {
        this.buf = buf;
        this.pos = 0;
        this.count = buf.length;
        return this;
    }

    public byte[] decompress(byte[] buf) throws IOException {
        return ByteStreams.toByteArray(gzis.setInputStream(fromBytes(buf)));
    }
}
