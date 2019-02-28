package wjc.bigdata.hadoop.secondarysort;

import org.apache.hadoop.io.Writable;
import wjc.bigdata.algorithm.utils.DateUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * NaturalValue represents a pair of
 * (timestamp, price).
 *
 * @author Mahmoud Parsian
 */
public class NaturalValue
        implements Writable, Comparable<NaturalValue> {

    private long   timestamp;
    private double price;

    public NaturalValue(long timestamp, double price) {
        set(timestamp, price);
    }

    public NaturalValue() {
    }

    public static NaturalValue copy(NaturalValue value) {
        return new NaturalValue(value.timestamp, value.price);
    }

    /**
     * Convert a binary data into NaturalValue
     *
     * @param in A DataInput object to read from.
     * @return A NaturalValue object
     * @throws IOException
     */
    public static NaturalValue read(DataInput in) throws IOException {
        NaturalValue value = new NaturalValue();
        value.readFields(in);
        return value;
    }

    public void set(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getPrice() {
        return this.price;
    }

    /**
     * Deserializes the point from the underlying data.
     *
     * @param in a DataInput object to read the point from.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.price = in.readDouble();
    }

    public String getDate() {
        return DateUtil.getDateAsString(this.timestamp);
    }

    /**
     * Creates a clone of this object
     */
    @Override
    public NaturalValue clone() {
        return new NaturalValue(timestamp, price);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timestamp);
        out.writeDouble(this.price);

    }

    /**
     * Used in sorting the data in the reducer
     */
    @Override
    public int compareTo(NaturalValue data) {
        return Long.compare(this.timestamp, data.timestamp);
    }

}
