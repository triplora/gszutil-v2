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
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;

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

        public Source(ReadableByteChannel in, int blkSize){
            this.in = in;
            this.blkSize = blkSize;
        }

        @Override
        public UploadResult call() throws IOException {
            byte[] data = new byte[blkSize];
            ByteBuffer bb = ByteBuffer.wrap(data);
            try (ZContext ctx = new ZContext()){
                try (Socket client = ctx.createSocket(SocketType.DEALER)){
                    client.connect("tcp://127.0.0.1:5570");
                    client.setLinger(-1);
                    client.setHWM(64);
                    while (!Thread.currentThread().isInterrupted()) {
                        bb.clear();
                        if (in.read(bb) < 0) {
                            break;
                        }

                        if (bb.position() > 0) {
                            byte[] payload = Arrays.copyOf(data, bb.position());

                            // Send Null Frame to be removed by REP
                            client.send(new byte[0], ZMQ.SNDMORE);

                            // Send payload
                            client.send(payload, 0);

                            // Increment counters
                            bytesOut += payload.length;
                            bytesIn += payload.length;
                        }
                    }
                }
            }
            return new UploadResult(bytesIn, bytesOut);
        }
    }

    public static class Router implements Runnable {
        private String uri;
        private String backendUri;
        private ZContext ctx;
        public Router(ZContext ctx, int port, String backendUri){
            this.uri = "tcp://*:" + port;
            this.backendUri = backendUri;
            this.ctx = ctx;
        }
        @Override
        public void run() {
            //  Frontend socket talks to clients over TCP
            Socket frontend = ctx.createSocket(SocketType.ROUTER);
            frontend.bind(uri);

            //  Backend socket talks to workers over inproc
            Socket backend = ctx.createSocket(SocketType.DEALER);
            backend.bind(backendUri);

            //  Connect backend to frontend via a proxy
            ZMQ.proxy(frontend, backend, null);
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
        private ZContext ctx;
        private long bytesIn = 0L;
        private String uri;

        public Sink(ZContext ctx, String uri) {
            this.ctx = ctx;
            this.uri = uri;
        }

        @Override
        public WriteResult call() {
            try (Socket socket = ctx.createSocket(SocketType.REP)) {
                socket.connect(uri);
                while (!Thread.currentThread().isInterrupted()) {
                    byte[] data = socket.recv(0);
                    if (data != null && data.length > 0) {
                        bytesIn += data.length;
                    } else break;
                }
            }
            return new WriteResult(bytesIn);
        }
    }

    // start client and server
    // wait for server to finish

    public static class RandomBytes implements ReadableByteChannel {
        private final long size;
        private final Random random = new Random();
        private long consumed = 0;
        public RandomBytes(long size){
            this.size = size;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (consumed > size)
                return -1;
            random.nextBytes(dst.array());
            consumed += dst.capacity();
            dst.position(dst.limit());
            return dst.capacity();
        }

        private boolean open = true;

        @Override
        public void close() throws IOException {
            open = false;
        }

        @Override
        public boolean isOpen() {
            return open;
        }
    }
}
