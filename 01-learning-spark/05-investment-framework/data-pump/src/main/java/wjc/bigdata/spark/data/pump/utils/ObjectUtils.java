package wjc.bigdata.spark.data.pump.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.io.Serializable;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 14:02
 **/
public class ObjectUtils implements Serializable {
    private final static Logger logger = LoggerFactory.getLogger(ObjectUtils.class);

    public static <T> T get(DefaultListableBeanFactory factory, Class<T> clazz, T defValue) {

        try {
            return factory.getBean(clazz);
        } catch (Exception e) {
            logger.warn("can not find instance of class " + clazz);
        }

        return defValue;
    }
}
