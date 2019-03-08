/**
 * Illustrates Kryo serialization in Java
 */
package wjc.bigdata.spark.allexamples;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.Arrays;

public final class BasicAvgWithKryo {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        SparkConf conf = new SparkConf().setMaster(master).setAppName("basic-avg-with-kyro");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", AvgRegistrator.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, Integer x) {
                a.total += x;
                a.num += 1;
                return a;
            }
        };
        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) {
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };
        AvgCount initial = new AvgCount(0, 0);
        AvgCount result = rdd.aggregate(initial, addAndCount, combine);
        System.out.println(result.avg());
    }

    // This is our custom class we will configure Kyro to serialize
    static class AvgCount implements java.io.Serializable {
        public int total;
        public int num;

        public AvgCount() {
            total = 0;
            num = 0;
        }

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public float avg() {
            return total / (float) num;
        }
    }

    public static class AvgRegistrator implements KryoRegistrator {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(AvgCount.class, new FieldSerializer(kryo, AvgCount.class));
        }
    }
}
