package wjc.bigdata.spark.data.pump.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wjc.bigdata.spark.data.pump.connector.Input;
import wjc.bigdata.spark.data.pump.connector.Output;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-29 08:44
 **/
public class DefaultHandlerChain implements HandlerChain {
    private final static Logger        logger   = LoggerFactory.getLogger(DefaultHandlerChain.class);
    private              List<Handler> handlers = new ArrayList<>();
    private              int           index    = 0;

    public DefaultHandlerChain() {
    }

    public DefaultHandlerChain(List<Handler> handlers) {
        Optional.ofNullable(handlers)
                .ifPresent(index -> index.stream()
                        .filter(Objects::nonNull)
                        .forEach(x -> this.handlers.add(x)));

    }

    void addHandler(final Handler handler) {
        if (!handlers.contains(handler)) {
            handlers.add(handler);
        }
    }

    @Override
    public void handle(Input inputConnector, Output output) {
        if (index < handlers.size()) {
            Handler handler = handlers.get(index);
            ++index;
            handler.handle(inputConnector, output, this);
        }
    }
}
