package  org.cord.ignite;

import org.apache.ignite.Ignite;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * Created by cord on 2018/4/11.
 */
@SpringBootApplication
//@ImportResource(locations={"classpath:ignite-config.xml"})
@ImportResource(locations={"${ignite.config}"})
@MapperScan("org.cord.ignite.data.mapper")
public class StartApplication implements CommandLineRunner{

    @Autowired
    private Ignite ignite;

//    @Autowired
//    private DataLoader dataLoader;


    /** 启动主方法*/
    public static void main(String[] args) {
        SpringApplication.run(StartApplication.class, args);
    }

    /** 启动完成之后执行初始化*/
    @Override
    public void run(String... strings) throws Exception {
        if(!ignite.cluster().active()){
            ignite.cluster().active(true);    //如果集群未启动则启动集群
        }

        //加载数据
//        dataLoader.loadData();
    }
}
