package wjc.bigdata.spark.data.pump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import wjc.bigdata.spark.data.pump.operation.AfterCalculateHandler;
import wjc.bigdata.spark.data.pump.operation.BeforeCalculationHandler;
import wjc.bigdata.spark.data.pump.operation.CalculationContext;
import wjc.bigdata.spark.data.pump.operation.CalculatorChain;
import wjc.bigdata.spark.data.pump.utils.ObjectUtils;

import java.io.Serializable;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 16:52
 **/
public class SparkApplication implements Serializable {
    private final static Logger logger = LoggerFactory.getLogger(SparkApplication.class);

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

        CalculatorChain handlerChain = factory.getBean(CalculatorChain.class);

        BeforeCalculationHandler beforeHandler = ObjectUtils.get(factory, BeforeCalculationHandler.class, null);
        AfterCalculateHandler afterHandler = ObjectUtils.get(factory, AfterCalculateHandler.class, null);

        CalculationContext context = factory.getBean(CalculationContext.class);
        handlerChain.beforeHandle(beforeHandler);
        handlerChain.handle(context);
        handlerChain.afterHandle(afterHandler);
    }


}
