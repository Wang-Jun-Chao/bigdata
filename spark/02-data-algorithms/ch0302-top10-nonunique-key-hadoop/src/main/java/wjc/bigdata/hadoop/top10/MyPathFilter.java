package wjc.bigdata.hadoop.top10;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-02-16 20:18
 **/
public class MyPathFilter implements PathFilter {
    private static Logger THE_LOGGER = LoggerFactory.getLogger(MyPathFilter.class);

    @Override
    public boolean accept(Path path) {
        THE_LOGGER.error(path.getName());
        return !path.getName().equals("_SUCCESS");

    }
}
