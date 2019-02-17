package wjc.bigdata.spark.data.pump.operation;

import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;
import wjc.bigdata.spark.data.pump.metrix.Metrix;

import java.io.Serializable;
import java.util.Collection;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-03 07:21
 **/
public interface CalculationContext extends Serializable {

    void addSource(Metrix<?> metrix);

    void addSource(Collection<Metrix<?>> metrixs);

    void setResult(Collection<Metrix<?>> metrix);

    Collection<Metrix<?>> getResult();

    Input getInput();

    Output getOutput();
}
