package wjc.bigdata.hive.ch13function;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-05-31 13:07
 **/
public class GenericUDTFFor extends GenericUDTF {
    private IntWritable start;
    private IntWritable end;
    private IntWritable inc;
    private Object[]    forwardObj = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        start = ((WritableConstantIntObjectInspector) args[0]).getWritableConstantValue();
        end = ((WritableConstantIntObjectInspector) args[1]).getWritableConstantValue();
        if (args.length == 3) {
            inc = ((WritableConstantIntObjectInspector) args[2]).getWritableConstantValue();
        } else {
            inc = new IntWritable(1);
        }

        this.forwardObj = new Object[1];
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOis = new ArrayList<>();
        fieldNames.add("col0");
        fieldOis.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.INT));
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOis);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        for (int i = start.get(); i <= end.get(); i = i + inc.get()) {
            this.forwardObj[0] = i;
            forward(forwardObj);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
