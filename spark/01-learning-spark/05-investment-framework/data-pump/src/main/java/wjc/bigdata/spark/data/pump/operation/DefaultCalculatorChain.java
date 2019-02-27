package wjc.bigdata.spark.data.pump.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 08:44
 **/
public class DefaultCalculatorChain implements CalculatorChain {
    private final static Logger           logger      = LoggerFactory.getLogger(DefaultCalculatorChain.class);
    private              List<Calculator> calculators = new ArrayList<>();
    private              int              index       = 0;

    public DefaultCalculatorChain() {
    }

    public DefaultCalculatorChain(List<Calculator> calculators) {
        Optional.ofNullable(calculators)
                .ifPresent(index -> index.stream()
                        .filter(Objects::nonNull)
                        .forEach(x -> this.calculators.add(x)));

    }

    void addHandler(final Calculator handler) {
        if (!calculators.contains(handler)) {
            calculators.add(handler);
        }
    }


    @Override
    public void beforeHandle(BeforeCalculationHandler handler) {
        if (handler != null) {
            handler.handle();
        }
    }

    @Override
    public void handle(CalculationContext context) {
        if (index < calculators.size()) {
            Calculator handler = calculators.get(index);
            ++index;
            handler.handle(context, this);
        }
    }

    @Override
    public void afterHandle(AfterCalculateHandler handler) {
        if (handler != null) {
            handler.handle();
        }
    }
}
