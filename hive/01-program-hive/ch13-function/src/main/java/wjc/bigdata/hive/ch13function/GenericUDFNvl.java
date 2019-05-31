package wjc.bigdata.hive.ch13function;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-05-31 08:13
 **/
@Description(name = "nvl",
        value= "_FUNC_(value, default_value) - Returns default value if value is null else returns value",
        extended= "Example:\n"
                +" > SELECT _FUNC_(null,'bla') FROM src LIMIT 1;\n")
public class GenericUDFNvl extends GenericUDF {
    private final static Logger logger = LoggerFactory.getLogger(GenericUDFNvl.class);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
