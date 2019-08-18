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

import java.nio.ByteBuffer;

public class Binary {
    /**
     * Picture	Storage representation	Numeric values
     * S9(1)  to S9(4) COMP.  Binary halfword (2 bytes)
     *   -32768
     *   +32767
     * S9(5)  to S9(9) COMP.  Binary fullword (4 bytes)
     *   -2,147,483,648
     *   +2,147,483,647
     * S9(10) to S9(18) COMP. Binary doubleword (8 bytes)
     *   -9,223,372,036,854,775,808
     *   +9,223,372,036,854,775,807
     */
    public static long decode(ByteBuffer buf, int size) {
        long v = 0;
        int j = 0;
        byte b = buf.get(buf.position());
        boolean isNegative = (b & 0x80) == 0x80;

        if (isNegative) {
            while (j < size){
                b = buf.get();
                v <<= 8;
                v |= (~b & 0xFF);
                j ++;
            }
            v *= -1;
            v -= 1;
        } else {
            while (j < size){
                b = buf.get();
                v <<= 8;
                v |= (b & 0xFF);
                j ++;
            }
        }
        return v;
    }

    /**
     * Picture	Storage representation	Numeric values
     * 9(1) to 9(4)	    Binary halfword (2 bytes)	0 through 65535
     * 9(5) to 9(9)	    Binary fullword (4 bytes)	0 through 4,294,967,295
     * 9(10) to 9(18)	Binary doubleword (8 bytes)	0 through 18,446,744,073,709,551,615
     */
    public static long decodeUnsigned(ByteBuffer buf, int size) {
        long v = 0;
        int j = 0;
        byte b;
        while (j < size){
            b = buf.get();
            v <<= 8;
            v |= (b & 0xFF);
            j ++;
        }
        return v;
    }

    public static String binValue(byte b) {
        StringBuilder sb = new StringBuilder(8);
        for (int i = 0; i < 8; i++) {
            if ((b & 0x01) == 0x01) {
                sb.append('1');
            } else {
                sb.append('0');
            }
            b >>>= 1;
        }
        return sb.reverse().toString();
    }

    public static String binValue(byte[] a) {
        StringBuilder sb = new StringBuilder(8*a.length);
        for (int j = 0; j< a.length; j++){
            sb.append(binValue(a[j]));
        }
        return sb.toString();
    }
}
