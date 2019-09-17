/*
 * Copyright 2019 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gszutil.io;

import com.google.common.base.Preconditions;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.zip.Deflater;

public class Service {
    public static class UploadResult {
        private long bytesIn;
        private long bytesOut;
        public UploadResult(long in, long out){
            bytesIn = in;
            bytesOut = out;
        }

        public long getBytesIn() {
            return bytesIn;
        }

        public long getBytesOut() {
            return bytesOut;
        }
    }

    public static class Source implements Callable<UploadResult> {
        private final ReadableByteChannel in;
        private long bytesIn = 0;
        private long bytesOut = 0;
        private final int blkSize;
        private final Socket[] sockets;

        public Source(ReadableByteChannel in, int blkSize,Socket[] sockets){
            for (Socket s : sockets){
                Preconditions.checkArgument(
                    s.getSocketType() == SocketType.DEALER,
                    "SocketType must be DEALER"
                );
            }
            this.in = in;
            this.blkSize = blkSize;
            this.sockets = sockets;
        }

        @Override
        public UploadResult call() throws IOException {
            final byte[] data = new byte[blkSize];
            final ByteBuffer bb = ByteBuffer.wrap(data);
            final byte[] buf = new byte[blkSize+4096];
            final Deflater deflater = new Deflater();
            deflater.setLevel(2);

            int compressed;
            int i = 0;
            while (!Thread.currentThread().isInterrupted()) {
                bb.clear();
                if (in.read(bb) < 0) {
                    break;
                }

                if (bb.position() > 0) {
                    bytesIn += bb.position();
                    deflater.setInput(data, 0, bb.position());
                    compressed = deflater.deflate(buf, 0, buf.length, Deflater.SYNC_FLUSH);

                    if (compressed > 0) {
                        i++;
                        if (i >= sockets.length)
                            i = 0;

                        Socket socket = sockets[i];

                        // Send payload
                        socket.send(Arrays.copyOf(buf, compressed), 0);

                        // Increment counters
                        bytesOut += compressed;
                    }
                }
            }
            return new UploadResult(bytesIn, bytesOut);
        }
    }

    public static class WriteResult {
        private long bytesIn;
        public WriteResult(long in){
            bytesIn = in;
        }

        public long getBytesIn() {
            return bytesIn;
        }
    }

    public static class Sink implements Callable<WriteResult> {
        private long bytesIn = 0L;
        private final Socket socket;

        public Sink(Socket socket) {
            Preconditions.checkArgument(socket.getSocketType() == SocketType.ROUTER, "SocketType must be ROUTER");
            this.socket = socket;
        }

        @Override
        public WriteResult call() {
            while (!Thread.currentThread().isInterrupted()) {
                byte[] data = socket.recv(0);
                if (data != null && data.length > 0) {
                    bytesIn += data.length;
                } else return new WriteResult(bytesIn);
            }
            return null;
        }
    }

    // start client and server
    // wait for server to finish
    public static class RandomBytes implements ReadableByteChannel {
        private final long size;
        private long consumed = 0;
        public RandomBytes(long size){
            this.size = size;
        }

        @Override
        public int read(ByteBuffer dst) {
            if (consumed >= size)
                return -1;
            while(dst.remaining() >= 4096){
                dst.putLong(88);
                dst.putLong(42);
            }
            while(dst.remaining() >= 8){
                dst.putLong(21);
            }
            while (dst.hasRemaining()){
                dst.putChar('x');
            }
            consumed += dst.position();
            return dst.position();
        }

        private boolean open = true;

        @Override
        public void close() {
            open = false;
        }

        @Override
        public boolean isOpen() {
            return open;
        }
    }
}
