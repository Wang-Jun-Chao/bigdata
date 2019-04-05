package wjc.spark.ignite.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-04-05 07:29
 **/
@SpringBootApplication
@ImportResource(locations = {"classpath:default-config.xml"})
public class DatabaseApplication {
    private final static Logger logger = LoggerFactory.getLogger(DatabaseApplication.class);
}
