package com.ct.common.constant;

import com.ct.common.bean.Val;

/**
 * 名称常量枚举类
 */
public enum Names implements Val {
    // 提供当前枚举类的对象，多个对象之间用，隔开，末尾对象;结束
    NAMESPACE("ct"),
    TABLE("ct:calllog"),
    CF_CALLER("caller"),
    CF_CALLEE("callee"),
    CF_INFO("info"),
    TOPIC("ct");

    private String name;

    Names(String name) {
        this.name = name;
    }

    public String getValue() {
        return name;
    }

    public void setValue(Object value) {
        this.name = (String) value;
    }
}
