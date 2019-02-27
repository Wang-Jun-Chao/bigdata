package wjc.bigdata.spark.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.operation.BeforeCalculationHandler;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 22:38
 **/
public class TestBeforeCalculationHandler implements BeforeCalculationHandler {
    private final static Logger logger = LoggerFactory.getLogger(TestBeforeCalculationHandler.class);

    @Override
    public void handle() {
        System.out.println(this.getClass().getName());
    }
}
