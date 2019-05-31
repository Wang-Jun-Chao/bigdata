package wjc.bigdata.hive.ch13function;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-05-31 08:13
 **/
@Description(name = "nvl",
        value = "_FUNC_(value, default_value) - Returns default value if value is null else returns value",
        extended = "Example:\n"
                + " > SELECT _FUNC_(null,'bla') FROM src LIMIT 1;\n")
public class GenericUDFNvl extends GenericUDF {
    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[]                             argumentOIs;

    /**
     * 其中initialize()方法会被输入的每个参数调用，并最终传入到一个ObjectInspector
     * 对象中。这个方法的目标是确定参数的返回类型。如果传入方法的类型是不合法
     * 的，这时用户同样可以向控制台抛出一个Exception 异常信息。returnOIResolver
     * 是一个内置的类，其通过获取非null 值的变量的类型并使用这个数据类型来确定
     * 返回值类型：
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments;
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The operator'NVL'accepts 2 arguments.");
        }

        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        if (!(returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1]))) {
            throw new UDFArgumentTypeException(2,
                    "The 1st and 2nd args of function NLV should have the same type, "
                            + "but they are different: \"" + arguments[0].getTypeName()
                            + "\" and \"" + arguments[1].getTypeName() + "\"");
        }
        return returnOIResolver.get();
    }

    /**
     * 方法evaluate 的输入是一个DeferredObject 对象数组，而initialize 方法中创建的
     * returnOIResolver 对象就用于从DeferredObjects 对象中获取到值。在这种情况下，这个
     * 函数将会返回第1 个非null 值：
     *
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object retVal = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
        if (retVal == null) {
            retVal = returnOIResolver.convertIfNecessary(arguments[1].get(), argumentOIs[1]);
        }
        return retVal;
    }

    /**
     * 用于Hadoop task 内部，在使用到这个函数时来展示调试信息
     *
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        return "if " + children[0] + " is null returns " + children[1];
    }
}
