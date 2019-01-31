package wjc.bigdata.spark.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.operation.BeforeAction;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 22:38
 **/
public class TestBeforeAction implements BeforeAction {
    private final static Logger logger = LoggerFactory.getLogger(TestBeforeAction.class);

    @Override
    public void act() {
        System.out.println(this.getClass().getName());
    }
}