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

package com.google.cloud.imf.gzos;

import com.ibm.dataaccess.DecimalData;

import java.nio.ByteBuffer;

public class PackedDecimal {
    private static final int SIGN_SIZE = 1;

    public static int sizeOf(int p, int s) {
        return (p + s) / 2 + 1;
    }

    public static int precisionOf(int size) {
        return size * 2 - SIGN_SIZE;
    }

    public static long unpack(ByteBuffer buf, int len) {
        int startPos = buf.position();
        buf.position(startPos + len);
        return unpack(buf.array(), startPos, len);
    }

    public static long unpack(byte[] buf, int pos, int len) {
        long x = 0;
        int k;
        int a; // first half-byte nibble
        int b; // second half-byte nibble
        int i = pos;
        int limit = pos + len - 1;
        while (i < limit) {
            // get byte as unsigned integer
            k = buf[i];
            if (k < 0) k += 256;

            // get hex digit values
            a = k >>> 4;
            b = k & 0x0F;

            // validate hex digit values
            //if (a > 9 || b > 9)
            //    throw new IllegalArgumentException("Invalid hex digit value " + a + " " + b);

            // add to result
            x += a;
            x *= 10L;
            x += b;
            x *= 10L;
            i += 1;
        }

        // get last byte as unsigned integer
        k = buf[i];
        if (k < 0) k += 256;

        // get hex digit values
        a = k >>> 4;
        b = k & 0x0F;
        //if (a > 9)
        //    throw new IllegalArgumentException("Invalid hex digit value " + a);

        // add digit from first nibble
        x += a;

        // get sign from second nibble - See PackedDecimal.sign and CommonData.getSign
        int sign = (b != 0x0D && b != 0x0B) ? 1 : -1;
        x *= sign;
        return x;
    }

    public static byte[] pack(long x, int len) {
        return pack(x, len, new byte[len], 0);
    }

    public static byte[] pack(long x, int len, byte[] buf, int off) {
        DecimalData.convertLongToPackedDecimal(x, buf, off, precisionOf(len), true);
        return buf;
    }
}
