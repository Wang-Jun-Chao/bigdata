package wjc.bigdata.hadoop.movingaverage.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * NaturalKeyGroupingComparator
 * <p>
 * This class is used during Hadoop's shuffle phase to
 * group composite key's by the first part (natural) of
 * their key.
 * <p>
 * The natural key for time series data is the "name".
 *
 * @author Mahmoud Parsian
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;
        return key1.getName().compareTo(key2.getName());
    }

}
