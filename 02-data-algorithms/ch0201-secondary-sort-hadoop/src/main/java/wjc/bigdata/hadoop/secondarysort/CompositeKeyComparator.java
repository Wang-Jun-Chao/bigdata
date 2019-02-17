package wjc.bigdata.hadoop.secondarysort;

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
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CompositeKey ck1 = (CompositeKey) wc1;
        CompositeKey ck2 = (CompositeKey) wc2;

        int comparison = ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
        if (comparison == 0) {
            // stock symbols are equal here
            return Long.compare(ck1.getTimestamp(), ck2.getTimestamp());
        } else {
            return comparison;
        }
    }
}
