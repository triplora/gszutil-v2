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

package com.google.cloud.gszutil;

import com.google.cloud.imf.util.Bits;
import com.google.cloud.imf.io.Bytes;

import java.nio.ByteBuffer;

public class PackedDecimal {
    public static final boolean relaxedParsing = !System.getProperty("java.vm.vendor").contains("IBM");

    public static int sizeOf(int p, int s) {
        return ((p + s) / 2) + 1;
    }

    public static int precisionOf(int size) {
        return (size - 1) * 2;
    }

    public static long unpack(ByteBuffer buf, int len) {
        long x = 0;
        for (int i = 0; i < len - 1; i++) {
            byte b = buf.get();
            x += Bits.uint(b) >>> 4;
            x *= 10L;
            x += Bits.uint(b) & 0xF;
            x *= 10L;
        }
        byte b = buf.get();
        x += Bits.uint(b) >>> 4;
        int sign = Bits.uint(b) & 0xF ;
        if (sign == 0xD) { x *= -1L; }
        else if (sign == 0xC) { /*positive*/ }
        else if (sign == 0xF) { /*unsigned*/ }
        else {
            if (!relaxedParsing) {
                byte[] a = new byte[len];
                int startPos = buf.position() - len;
                buf.position(startPos);
                buf.get(a);
                String hex = Bytes.hexValue(a);
                throw new IllegalArgumentException("unexpected sign bits " + sign + "\n" + hex);
            }
        }
        return x;
    }
}
