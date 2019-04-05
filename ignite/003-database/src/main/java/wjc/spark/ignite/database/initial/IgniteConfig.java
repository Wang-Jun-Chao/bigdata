package wjc.spark.ignite.database.initial;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-04-05 14:27
 **/
@Configuration
public class IgniteConfig {
    @Autowired
    private IgniteConfiguration igniteCfg;

    @Bean
    @ConditionalOnMissingBean
    public Ignite initIgnite() {
        // 推荐借助spring bean的方式注入ignite配置信息,只需要将配置xml文件import即可
        // 启动类加上注解@ImportResource(locations={"classpath:default-config.xml"})
        // Ignite ignite = Ignition.start(classpath:default-config.xml)
        return Ignition.start(igniteCfg);
    }
}
