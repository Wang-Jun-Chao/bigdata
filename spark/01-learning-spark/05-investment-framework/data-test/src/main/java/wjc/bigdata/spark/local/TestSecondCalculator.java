package wjc.bigdata.spark.local;

import wjc.bigdata.spark.data.pump.operation.CalculationContext;
import wjc.bigdata.spark.data.pump.operation.Calculator;
import wjc.bigdata.spark.data.pump.operation.CalculatorChain;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 19:53
 **/
public class TestSecondCalculator implements Calculator {

    @Override
    public void handle(CalculationContext context, CalculatorChain chain) {
        System.out.println(this.getClass().getName());
        chain.handle(context);
    }
}
