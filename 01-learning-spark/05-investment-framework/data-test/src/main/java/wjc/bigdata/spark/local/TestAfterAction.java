package wjc.bigdata.spark.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.operation.AfterAction;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 22:38
 **/
public class TestAfterAction implements AfterAction {
    private final static Logger logger = LoggerFactory.getLogger(TestAfterAction.class);

    @Override
    public void act() {
        System.out.println(this.getClass().getName());
    }
}
