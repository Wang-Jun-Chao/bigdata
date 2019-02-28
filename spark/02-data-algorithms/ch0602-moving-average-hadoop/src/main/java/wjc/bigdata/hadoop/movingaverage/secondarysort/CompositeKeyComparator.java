package wjc.bigdata.hadoop.movingaverage.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * CompositeKeyComparator
 * <p>
 * The purpose of this class is to enable comparison of two CompositeKey(s).
 *
 * @author Mahmoud Parsian
 */
public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;

        int comparison = key1.getName().compareTo(key2.getName());
        if (comparison == 0) {
            // names are equal here
            return Long.compare(key1.getTimestamp(), key2.getTimestamp());
        } else {
            return comparison;
        }
    }
}
