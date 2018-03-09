package global;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class Convert {
    public static void setIntValue(int value, int position, byte[] data)
            throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(out);
        outputStream.writeInt(value);
        byte[] B = ((ByteArrayOutputStream) out).toByteArray();
        System.arraycopy(B, 0, data, position, 4);
    }

    public static int getIntValue(int position, byte[] data)
            throws IOException {
        InputStream in;
        DataInputStream instr;
        int value;
        byte tmp[] = new byte[4];
        System.arraycopy(data, position, tmp, 0, 4);
        in = new ByteArrayInputStream(tmp);
        instr = new DataInputStream(in);
        value = instr.readInt();
        return value;
    }

    public static void setStringValue(String value, int position, byte[] data)
            throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream outstr = new DataOutputStream(out);
        outstr.writeUTF(value);
        byte[] B = ((ByteArrayOutputStream) out).toByteArray();
        int sz = outstr.size();
        System.arraycopy(B, 0, data, position, sz);
    }

    public static float getFloatValue(int position, byte[] data)
            throws IOException {
        InputStream in;
        DataInputStream instr;
        float value;
        byte tmp[] = new byte[4];
        System.arraycopy(data, position, tmp, 0, 4);
        in = new ByteArrayInputStream(tmp);
        instr = new DataInputStream(in);
        value = instr.readFloat();
        return value;
    }

    public static short getShortValue(int position, byte[] data)
            throws IOException {
        InputStream in;
        DataInputStream instr;
        short value;
        byte tmp[] = new byte[2];
        System.arraycopy(data, position, tmp, 0, 2);
        in = new ByteArrayInputStream(tmp);
        instr = new DataInputStream(in);
        value = instr.readShort();
        return value;
    }

    public static char getCharValue(int position, byte[] data)
            throws IOException {
        InputStream in;
        DataInputStream instr;
        char value;
        byte tmp[] = new byte[2];
        System.arraycopy(data, position, tmp, 0, 2);
        in = new ByteArrayInputStream(tmp);
        instr = new DataInputStream(in);
        value = instr.readChar();
        return value;
    }

    public static void setFloatValue(float value, int position, byte[] data)
            throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream outstr = new DataOutputStream(out);
        outstr.writeFloat(value);
        byte[] B = ((ByteArrayOutputStream) out).toByteArray();
        System.arraycopy(B, 0, data, position, 4);
    }

    public static byte[] intAtobyteA(int[] data) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * 4);
        IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(data);

        byte[] array = byteBuffer.array();
        return array;
    }

    public static void setShortValue(short value, int position, byte[] data)
            throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream outstr = new DataOutputStream(out);
        outstr.writeShort(value);
        byte[] B = ((ByteArrayOutputStream) out).toByteArray();
        System.arraycopy(B, 0, data, position, 2);
    }

    public static long getLongValue(int position, byte[] data)
            throws IOException {
        InputStream in;
        DataInputStream instr;
        long value;
        byte tmp[] = new byte[8];
        System.arraycopy(data, position, tmp, 0, 8);
        in = new ByteArrayInputStream(tmp);
        instr = new DataInputStream(in);
        value = instr.readLong();
        return value;
    }

    public static void setLongValue(long value, int position, byte[] data)
            throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream outstr = new DataOutputStream(out);
        outstr.writeLong(value);
        byte[] B = ((ByteArrayOutputStream) out).toByteArray();
        System.arraycopy(B, 0, data, position, 8);
    }

    public static String getStringValue(int position, byte[] data, int length)
            throws IOException {
        InputStream in;
        DataInputStream instr;
        String value;
        byte tmp[] = new byte[length];
        System.arraycopy(data, position, tmp, 0, length);
        in = new ByteArrayInputStream(tmp);
        instr = new DataInputStream(in);
        value = instr.readUTF();
        return value;
    }
}
