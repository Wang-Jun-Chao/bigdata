package wjc.bigdata.spark.data.platform.utils;

import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2019-01-28 17:49
 **/
public class User {
    private String              name;
    private int                 age;
    private Map<String, String> address;
    private String[]            roles;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Map<String, String> getAddress() {
        return address;
    }

    public void setAddress(Map<String, String> address) {
        this.address = address;
    }

    public String[] getRoles() {
        return roles;
    }

    public void setRoles(String[] roles) {
        this.roles = roles;
    }
}
