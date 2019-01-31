package wjc.bigdata.spark.data.platform;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import wjc.bigdata.spark.data.pump.operation.AfterAction;
import wjc.bigdata.spark.data.pump.operation.BeforeAction;
import wjc.bigdata.spark.data.pump.operation.HandlerChain;
import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;

import java.io.Serializable;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 16:52
 **/
public class SparkApplication implements Serializable {

    private final static String CONFIG     = "application.xml";
    private final static String PROPERTIES = "application.properties";

    public static void run(Class<?> clazz, String[] args) {

        String config = CONFIG;
        String properties = PROPERTIES;

        if (args != null && args.length > 0) {
            config = args[0];
            if (args.length > 1) {
                properties = args[1];
            }
        }

        // 加载XML文件配置
        DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
        XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
        reader.loadBeanDefinitions(new ClassPathResource(config));

        // 加载properties文件配置，将XML文件中有占位符替换成properties文件中的内容
        ClassPathResource resource = new ClassPathResource(properties);
        if (resource.exists()) {
            // bring in some property values from a Properties file
            PropertyPlaceholderConfigurer cfg = new PropertyPlaceholderConfigurer();
            cfg.setLocation(resource);
            // now actually do the replacement
            cfg.postProcessBeanFactory(factory);
        }

        Input inputConnector = factory.getBean(Input.class);
        Output output = factory.getBean(Output.class);
        HandlerChain handlerChain = factory.getBean(HandlerChain.class);

        BeforeAction beforeAction = get(factory, BeforeAction.class, null);
        AfterAction afterAction = get(factory, AfterAction.class, null);

        handlerChain.beforeHandle(beforeAction);
        handlerChain.handle(inputConnector, output);
        handlerChain.afterHandle(afterAction);
    }

    private static <T> T get(DefaultListableBeanFactory factory, Class<T> clazz, T defValue) {
        T t = defValue;
        try {
            t = factory.getBean(clazz);
        } catch (Exception e) {
            // do nothing
        }

        return t;
    }
}
