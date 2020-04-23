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

    public static long[] pows = new long[]{
        1L,
        10L,
        100L,
        1000L,
        10000L,
        100000L,
        1000000L,
        10000000L,
        100000000L,
        1000000000L,
        10000000000L,
        100000000000L,
        1000000000000L,
        10000000000000L,
        100000000000000L,
        1000000000000000L,
        10000000000000000L,
        100000000000000000L,
        1000000000000000000L
    };

    public static int pow(long x){
        if      (x >= 100000000000000000L) return 18;
        else if (x >= 10000000000000000L ) return 17;
        else if (x >= 1000000000000000L  ) return 15;
        else if (x >= 100000000000000L   ) return 14;
        else if (x >= 10000000000000L    ) return 13;
        else if (x >= 1000000000000L     ) return 12;
        else if (x >= 100000000000L      ) return 11;
        else if (x >= 10000000000L       ) return 10;
        else if (x >= 1000000000L        ) return 9;
        else if (x >= 100000000L         ) return 8;
        else if (x >= 10000000L          ) return 7;
        else if (x >= 1000000L           ) return 6;
        else if (x >= 100000L            ) return 5;
        else if (x >= 10000L             ) return 4;
        else if (x >= 1000L              ) return 3;
        else if (x >= 100L               ) return 2;
        else if (x >= 10L                ) return 1;
        else if (x >= 1L                 ) return 0;
        else return -1;
    }

    public static byte[] pack(long x, int len){
        return pack(x,len,new byte[len],0);
    }

    public static byte[] pack(long x, int len, byte[] buf, int off){
        com.ibm.dataaccess.DecimalData.convertLongToPackedDecimal(x, buf, off,
                precisionOf(len), true);
        return buf;
    }
}
