package wjc.bigdata.spark.sencondarysort;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

//
//

/**
 * TupleComparatorDescending: how to sort in descending order
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 */
public class TupleComparatorDescending implements Serializable, Comparator<Tuple2<String, Integer>> {

    static final         TupleComparatorDescending INSTANCE         = new TupleComparatorDescending();
    private static final long                      serialVersionUID = 1287049512718728895L;

    private TupleComparatorDescending() {
    }


    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        if (o2._1.compareTo(o1._1) == 0) {
            return o2._2.compareTo(o1._2);
        } else {
            return o2._1.compareTo(o1._1);
        }
    }

}
