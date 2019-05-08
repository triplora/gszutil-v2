package com.google.cloud.gszutil;


import com.google.common.base.Preconditions;

public class PackedDecimal {
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

    public static long unpack(byte[] bytes) {
        return unpack(bytes, 0, bytes.length);
    }

    public static long unpack(byte[] bytes, int off, int len) {
        Preconditions.checkNotNull(bytes);
        Preconditions.checkArgument(bytes.length > 0);
        long x = 0;
        for (int i = off; i < off + len - 1; i++) {
            x += uint(bytes[i]) >>> 4;
            x *= 10L;
            x += uint(bytes[i]) & 0xF;
            x *= 10L;
        }
        x += uint(bytes[off + len - 1]) >>> 4;
        int sign = uint(bytes[off + len - 1]) & 0xF ;
        if (sign == 0xD) { x *= -1L; }
        else if (sign == 0xC) { /*positive*/ }
        else if (sign == 0xF) { /*unsigned*/ }
        else { throw new IllegalArgumentException("unexpected sign bits " + sign); }
        return x;
    }
}
