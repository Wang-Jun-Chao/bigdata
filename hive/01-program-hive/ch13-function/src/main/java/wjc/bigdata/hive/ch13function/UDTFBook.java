package wjc.bigdata.hive.ch13function;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-05-31 14:07
 **/
public class UDTFBook extends GenericUDTF {
    private Object[] forwardObj = null;
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) {
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("isbn");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING));
        fieldNames.add("title");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING));
        fieldNames.add("authors");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.STRING)));

        forwardObj = new Object[3];
        return ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        String parts = args[0].toString();
        String[] part = parts.split("\\|");
        forwardObj[0] = part[0];
        forwardObj[1] = part[1];
        forwardObj[2] = part[2].split(",");
        this.forward(forwardObj);
    }

    @Override
    public void close() throws HiveException {

    }
}
