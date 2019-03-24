package wjc.bigdata.spark.util;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-24 10:12
 **/
public class SparkUtils {
    public static <T> Seq<T> seq(List<T> inputList) {
        return JavaConverters.collectionAsScalaIterableConverter(new ArrayList<>(inputList)).asScala().toSeq();
    }

    public static <T> Seq<T> seq(T ... data) {
        return seq(Arrays.asList(data));
    }
}
