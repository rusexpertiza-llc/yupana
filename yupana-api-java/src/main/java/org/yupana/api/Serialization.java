package org.yupana.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Serialization {

    public static byte[] writeBytes(byte[] bytes) {
        byte[] sizeBytes = writeVLong(bytes.length);
        byte[] result = new byte[sizeBytes.length + bytes.length];
        System.arraycopy(sizeBytes, 0, result, 0, sizeBytes.length);
        System.arraycopy(bytes, 0, result, sizeBytes.length + 1, bytes.length);
        return result;
    }

    public static byte[] readBytes(ByteBuffer bb) {
        int size = readVInt(bb);
        byte[] data = new byte[size];
        bb.get(data);
        return data;
    }

    public static byte[] writeString(String s) {
        byte[] a = s.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer
            .allocate(a.length + 4)
            .putInt(a.length)
            .put(a)
            .array();
    }

    public static byte[] writeDouble(double d) {
        return ByteBuffer.allocate(8).putDouble(d).array();
    }

    public static double readDouble(ByteBuffer bb) {
        return bb.getDouble();
    }

    public static byte[] writeBoolean(boolean b) {
        return writeByte(b ? (byte) 1 : 0);
    }

    public static boolean readBoolean(ByteBuffer bb) {
        return readByte(bb) != 0;
    }

    public static String readString(ByteBuffer bb) {
        int length = bb.getInt();
        byte[] bytes = new byte[length];
        bb.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] writeByte(byte b) {
        byte[] res = new byte[1];
        res[0] = b;
        return res;
    }

    public static byte readByte(ByteBuffer bb) {
        return bb.get();
    }

    public static byte[] writeVShort(short s) {
        return writeVLong(s);
    }

    public static short readVShort(ByteBuffer bb) {
        long l = readVLong(bb);
        if (l < Short.MIN_VALUE || l > Short.MAX_VALUE) throw new IllegalArgumentException("Got Long but Short expected");
        return (short) l;
    }

    public static byte[] writeVInt(int i) {
        return writeVLong(i);
    }

    public static int readVInt(ByteBuffer bb) {
        long l = readVLong(bb);
        if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) throw new IllegalArgumentException("Got Long but Int expected");
        return (int) l;
    }

    public static BigDecimal readBigDecimal(ByteBuffer bb) {
        int scale = readVInt(bb);
        int size = readVInt(bb);
        byte[] bytes = new byte[size];
        bb.get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    public static byte[] writeBigDecimal(BigDecimal x) {
        byte[] a = x.unscaledValue().toByteArray();
        byte[] scale = writeVLong(x.scale());
        byte[] length = writeVLong(a.length);
        return ByteBuffer
            .allocate(a.length + scale.length + length.length)
            .put(scale)
            .put(length)
            .put(a)
            .array();
    }

    public static byte[] writeVLong (long l) {
        if (l <= 127 && l > -112) {
            byte[] res = new byte[1];
            res[0] = (byte)l;
            return res;
        } else {
            long ll = l;
            ByteBuffer bb = ByteBuffer.allocate(9);
            int len = -112;

            if (ll < 0) {
                len = -120;
                ll ^= -1L;
            }

            long tmp = ll;
            while (tmp != 0) {
                tmp >>= 8;
                len -= 1;
            }

            bb.put((byte)len);

            len = (len < -120) ? -(len + 120) : -(len + 112);

            for (int idx = len - 1; idx >= 0; idx--) {
                int shift = idx * 8;
                long mask = 0xFFL << shift;
                bb.put((byte) ((ll & mask) >> shift));
            }

            byte[] res = new byte[bb.position()];
            bb.rewind();
            bb.get(res);
            return res;
        }
    }

    public static long readVLong(ByteBuffer bb) {
        byte first = bb.get();
        int len = (first >= -112) ? 1 :  (first >= -120) ? -111 - first : -119 - first;

        long result = 0L;

        if (len == 1) {
            return first;
        } else {

            for (int i = 0; i < len - 1; i++) {
                byte b = bb.get();
                result <<= 8;
                result |= (b & 0xff);
            }

            return (first >= -120) ? result : ~result;
        }
    }
}
