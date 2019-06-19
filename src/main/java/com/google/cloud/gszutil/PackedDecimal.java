/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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


import com.ibm.jzos.CrossPlatform;

import java.nio.ByteBuffer;

public class PackedDecimal {
    private static boolean relaxedParsing = !CrossPlatform.IBM();
    private static final String[] hexValues = new String[256];
    private static final char[] hex = new char[]{'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

    static {
        for (int i = 0; i < 256; i++) {
            hexValues[i] = String.valueOf(new char[]{
                hex[i >>> 4],
                hex[i & 0xF]
            });
        }
    }

    private static int uint(byte b) {
        if (b < 0) return 256 + b;
        else return b;
    }

    private static String hexValue(byte b) {
        return hexValues[uint(b)];
    }

    public static String hexValue(byte[] bytes) {
        StringBuilder sb1 = new StringBuilder(bytes.length * 2);
        StringBuilder sb2 = new StringBuilder(bytes.length * 2);
        for (int i = 0; i < bytes.length; i++){
            sb1.append(hexValue(bytes[i]).charAt(0));
            sb2.append(hexValue(bytes[i]).charAt(1));
        }
        return sb1.toString() + "\n" + sb2.toString();
    }

    public static long unpack(ByteBuffer buf, int len) {
        long x = 0;
        for (int i = 0; i < len - 1; i++) {
            byte b = buf.get();
            x += uint(b) >>> 4;
            x *= 10L;
            x += uint(b) & 0xF;
            x *= 10L;
        }
        byte b = buf.get();
        x += uint(b) >>> 4;
        int sign = uint(b) & 0xF ;
        if (sign == 0xD) { x *= -1L; }
        else if (sign == 0xC) { /*positive*/ }
        else if (sign == 0xF) { /*unsigned*/ }
        else {
            if (!relaxedParsing) {
                throw new IllegalArgumentException("unexpected sign bits " + sign);
            }
        }
        return x;
    }
}
