package wjc.bigdata.ignite.dataaccess;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-04-04 19:26
 **/
@SpringBootApplication
@ImportResource(locations={"classpath:default-config.xml"})
public class DataAccessApplication implements CommandLineRunner {
    @Autowired
    private IgniteConfiguration igniteCfg;

    public static void main(String[] args) {
        SpringApplication.run(DataAccessApplication.class, args);
    }

    /**启动完成之后执行初始化*/
    @Override
    public void run(String... strings) {
        //启动ignite服务
        Ignite ignite = Ignition.start(igniteCfg);
        //创建cache
        IgniteCache<String, String> cache =  ignite.getOrCreateCache("test");
        //存入数据
        cache.put("cord",  "hello");
        //查询数据
        System.out.format("key[%s]->value[%s]\n", "cord", cache.get("cord"));
    }
}
