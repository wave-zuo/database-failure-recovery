package cs245.as3;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Record {
    private short length;//题目保证最长128字节,byte最大127
    private long txID;
    private long key;
    private byte[] value;

    public Record(short length, long txID, long key, byte[] value) {
        this.length = length;
        this.txID = txID;
        this.key = key;
        this.value = value;
    }
    public Record(long txID, long key, byte[] value) {
        this.length = (short) (Short.BYTES + Long.BYTES + Long.BYTES + value.length);
        this.txID = txID;
        this.key = key;
        this.value = value;
    }

    public byte[] serialized() {
        // length+txid+key+value
        ByteBuffer record = ByteBuffer.allocate(length);
        record.putShort(length);
        record.putLong(txID);
        record.putLong(key);
        record.put(value);
        return record.array();
    }
    public static Record deserialized(byte[] record) {
        ByteBuffer buffer = ByteBuffer.wrap(record);
        short len = buffer.getShort();
        long txID = buffer.getLong();
        long key = buffer.getLong();
        byte[] val = new byte[len-(Short.BYTES + Long.BYTES + Long.BYTES)];
        buffer.get(val);
        return new Record(len, txID, key, val);
    }

    public long getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getTxID() {
        return txID;
    }

    public void print() {
        System.out.println("len="+length+", key="+key+", value="+ Arrays.toString(value));
    }

}
