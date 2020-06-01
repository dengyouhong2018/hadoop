package com.ct.common.bean;

/**
 * 数据对象
 */
public class Data implements Val{

    private String content;

    @Override
    public Object getValue() {
        return content;
    }

    @Override
    public void setValue(Object value) {
        this.content = (String) value;
    }

}
