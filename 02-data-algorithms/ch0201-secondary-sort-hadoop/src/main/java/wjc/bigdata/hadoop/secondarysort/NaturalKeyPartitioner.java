package wjc.bigdata.hadoop.secondarysort;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * NaturalKeyPartitioner
 * <p>
 * This custom partitioner allow us to distribute how outputs from the
 * map stage are sent to the reducers.  NaturalKeyPartitioner partitions
 * the data output from the map phase (SecondarySortMapper) before it is
 * sent through the shuffle phase. Since we want a single reducer to recieve
 * all time series data for a single "stockSymbol", we partition data output
 * of the map phase by only the natural key component ("stockSymbol").
 *
 * @author Mahmoud Parsian
 */
public class NaturalKeyPartitioner extends
        Partitioner<CompositeKey, NaturalValue> {

    /**
     * adapted from String.hashCode()
     */
    static long hash(String str) {
        // prime
        long h = 1125899906842597L;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31 * h + str.charAt(i);
        }
        return h;
    }

    @Override
    public int getPartition(CompositeKey key, NaturalValue value, int numberOfPartitions) {
        return Math.abs((int) (hash(key.getStockSymbol()) % numberOfPartitions));
    }
}
