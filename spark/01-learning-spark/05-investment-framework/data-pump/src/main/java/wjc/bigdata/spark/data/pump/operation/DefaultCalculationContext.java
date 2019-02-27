package wjc.bigdata.spark.data.pump.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;
import wjc.bigdata.spark.data.pump.metrix.Metrix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-09 15:56
 **/
public class DefaultCalculationContext implements CalculationContext {
    private final static Logger logger = LoggerFactory.getLogger(DefaultCalculationContext.class);

    private List<Metrix<?>>       source = new ArrayList<>();
    private Collection<Metrix<?>> result;
    private Input                 input;
    private Output                output;

    @Override
    public void addSource(Metrix<?> metrix) {
        source.add(metrix);
    }

    @Override
    public void addSource(Collection<Metrix<?>> metrixs) {
        source.addAll(metrixs);
    }

    @Override
    public void setResult(Collection<Metrix<?>> result) {
        this.result = result;
    }

    @Override
    public Collection<Metrix<?>> getResult() {
        return result;
    }

    @Override
    public Input getInput() {
        return input;
    }

    @Override
    public Output getOutput() {
        return output;
    }

    public List<? extends Metrix<?>> getSource() {
        return source;
    }

    public void setSource(List<Metrix<?>> source) {
        this.source = source;
    }

    public void setInput(Input input) {
        this.input = input;
    }

    public void setOutput(Output output) {
        this.output = output;
    }
}
