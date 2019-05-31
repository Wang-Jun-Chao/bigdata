package wjc.bigdata.hive.ch13function;


import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-05-31 13:33
 **/
public class Book implements Serializable {
    public String   isbn;
    public String   title;
    public String[] authors;

    public void fromString(String parts) {
        String[] part = parts.split("\\|");
        isbn = part[0];
        title = part[1];
        authors = part[2].split(",");
    }

    @Override
    public String toString() {
        return isbn + "\t" + title + "\t" + StringUtils.join(authors, ",");
    }
}
