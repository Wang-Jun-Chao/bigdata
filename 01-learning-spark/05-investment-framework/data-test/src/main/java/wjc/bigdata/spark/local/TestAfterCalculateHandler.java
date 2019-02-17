package wjc.bigdata.spark.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.operation.AfterCalculateHandler;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 22:38
 **/
public class TestAfterCalculateHandler implements AfterCalculateHandler {
    private final static Logger logger = LoggerFactory.getLogger(TestAfterCalculateHandler.class);

    @Override
    public void handle() {
        System.out.println(this.getClass().getName());
    }
}