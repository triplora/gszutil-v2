package com.google.cloud.gzos;

import sun.nio.cs.SingleByte;
import sun.nio.cs.ext.ExtendedCharsets;
import sun.nio.cs.ext.IBM1047;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

/** IBM1047 / CP1047 with 0xBA,0xBB '\u009A','\u009B' replaced with '[',']' (91,93) */
public class EBCDIC1 extends Charset {
    private static final String b2cTable = "Øabcdefghi«»ðýþ±°jklmnopqrªºæ¸Æ¤µ~stuvwxyz¡¿Ð[Þ®¬£¥·©§¶¼½¾Ý¨¯]´×{ABCDEFGHI\u00adôöòóõ}JKLMNOPQR¹ûüùúÿ\\÷STUVWXYZ²ÔÖÒÓÕ0123456789³ÛÜÙÚ\u009f\u0000\u0001\u0002\u0003\u009c\t\u0086\u007f\u0097\u008d\u008e\u000b\f\r\u000e\u000f\u0010\u0011\u0012\u0013\u009d\n\b\u0087\u0018\u0019\u0092\u008f\u001c\u001d\u001e\u001f\u0080\u0081\u0082\u0083\u0084\u0085\u0017\u001b\u0088\u0089\u008a\u008b\u008c\u0005\u0006\u0007\u0090\u0091\u0016\u0093\u0094\u0095\u0096\u0004\u0098\u0099\u009a\u009b\u0014\u0015\u009e\u001a  âäàáãåçñ¢.<(+|&éêëèíîïìß!$*);^-/ÂÄÀÁÃÅÇÑ¦,%_>?øÉÊËÈÍÎÏÌ`:#@'=\"";
    private static final char[] b2c0 = ("Øabcdefghi«»ðýþ±°jklmnopqrªºæ¸Æ¤µ~stuvwxyz" +
            "¡¿Ð[Þ®¬£¥·©§¶¼½¾Ý¨¯]´×{ABCDEFGHI\u00adôöòóõ}JKLMNOPQR¹ûüùúÿ\\÷STUVWXYZ²ÔÖÒÓÕ0123456789³ÛÜÙÚ\u009f\u0000\u0001\u0002\u0003\u009c\t\u0086\u007f\u0097\u008d\u008e\u000b\f\r\u000e\u000f\u0010\u0011\u0012\u0013\u009d\n\b\u0087\u0018\u0019\u0092\u008f\u001c\u001d\u001e\u001f\u0080\u0081\u0082\u0083\u0084\u0085\u0017\u001b\u0088\u0089\u008a\u008b\u008c\u0005\u0006\u0007\u0090\u0091\u0016\u0093\u0094\u0095\u0096\u0004\u0098\u0099\u009a\u009b\u0014\u0015\u009e\u001a  âäàáãåçñ¢.<(+|&éêëèíîïìß!$*);^-/ÂÄÀÁÃÅÇÑ¦,%_>?øÉÊËÈÍÎÏÌ`:#@'=\"").toCharArray();
    private static final char[] b2c = new char[]{
        'Ø','a','b','c','d','e','f','g','h','i','«','»','ð','ý','þ','±',
        '°','j','k','l','m','n','o','p','q','r','ª','º','æ','¸','Æ','¤',
        'µ','~','s','t','u','v','w','x','y','z','¡','¿','Ð','[','Þ','®',
        '¬','£','¥','·','©','§','¶','¼','½','¾','Ý','¨','¯',']','´','×',
        '{','A','B','C','D','E','F','G','H','I','\u00ad','ô','ö','ò','ó','õ',
        '}','J','K','L','M','N','O','P','Q','R','¹','û','ü','ù','ú','ÿ',
        '\\','÷','S','T','U','V','W','X','Y','Z','²','Ô','Ö','Ò','Ó','Õ',
        '0','1','2','3','4','5','6','7','8','9','³','Û','Ü','Ù','Ú','\u009F',
        '\u0000','\u0001','\u0002','\u0003','\u009C','\t','\u0086','\u007F',
            '\u0097','\u008D','\u008E','\u000B','\f','\n','\u000E','\u000F',
        '\u0010','\u0011','\u0012','\u0013','\u009D','\n','\b','\u0087',
            '\u0018','\u0019','\u0092','\u008F','\u001C','\u001D','\u001E','\u001F',
        '\u0080','\u0081','\u0082','\u0083','\u0084','\u0085','\u0017','\u001B',
            '\u0088','\u0089','\u008A','\u008B','\u008C','\u0005','\u0006','\u0007',
        '\u0090','\u0091','\u0016','\u0093','\u0094','\u0095','\u0096','\u0004',
            '\u0098','\u0099','[',']','\u0014','\u0015','\u009E','\u001A',
        ' ',' ','â','ä','à','á','ã','å','ç','ñ','¢','.','<','(','+','|',
        '&','é','ê','ë','è','í','î','ï','ì','ß','!','$','*',')',';','^',
        '-','/','Â','Ä','À','Á','Ã','Å','Ç','Ñ','¦',',','%','_','>','?',
        'ø','É','Ê','Ë','È','Í','Î','Ï','Ì','`',':','#','@','\'','=','"'
    };
    private static final char[] c2b = new char[256];
    private static final char[] c2bIndex = new char[256];

    public EBCDIC1() {
        super("EBCDIC1", ExtendedCharsets.aliasesFor("EBCDIC"));
    }

    public String historicalName() {
        return "EBCDIC";
    }

    public boolean contains(Charset var1) {
        return var1 instanceof IBM1047;
    }

    public CharsetDecoder newDecoder() {
        return new SingleByte.Decoder(this, b2c);
    }

    public CharsetEncoder newEncoder() {
        return new SingleByte.Encoder(this, c2b, c2bIndex);
    }

    static {
        char[] var0 = b2c;
        Object var1 = null;
        SingleByte.initC2B(var0, (char[])var1, c2b, c2bIndex);
    }
}
