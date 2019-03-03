package wjc.bigdata.spark.marketbasketanalysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Some methods for FindAssociationRules class.
 *
 * @author Mahmoud Parsian
 */
public class Util {

    static List<String> toList(String transaction) {
        String[] items = transaction.trim().split(",");
        return new ArrayList<String>(Arrays.asList(items));
    }

    static List<String> removeOneItem(List<String> list, int i) {
        if ((list == null) || (list.isEmpty())) {
            return list;
        }
        //
        if ((i < 0) || (i > (list.size() - 1))) {
            return list;
        }
        //
        List<String> cloned = new ArrayList<String>(list);
        cloned.remove(i);
        //
        return cloned;
    }

}
