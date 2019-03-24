package wjc.bigdata.spark.util;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-24 10:12
 **/
public class SparkUtils {
    public static <T> Seq<T> seq(List<T> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }
}
