package java.util.zip;

import java.io.IOException;
import java.io.OutputStream;

/** Created in java.util.zip package to access protected fields */
public class ResettableGzipOutputStream extends GZIPOutputStream {
    public ResettableGzipOutputStream(OutputStream out, int size, boolean syncFlush) throws IOException {
        super(out, size, syncFlush);
    }

    private final static int GZIP_MAGIC = 0x8b1f;

    /** should be called after reset() */
    public void writeHeader() throws IOException {
        out.write(new byte[] {
                (byte) GZIP_MAGIC,        // Magic number (short)
                (byte)(GZIP_MAGIC >> 8),  // Magic number (short)
                Deflater.DEFLATED,        // Compression method (CM)
                0,                        // Flags (FLG)
                0,                        // Modification time MTIME (int)
                0,                        // Modification time MTIME (int)
                0,                        // Modification time MTIME (int)
                0,                        // Modification time MTIME (int)
                0,                        // Extra flags (XFLG)
                0                         // Operating system (OS)
        });
    }

    public void reset() {
        crc.reset();
        def.reset();
    }
}
