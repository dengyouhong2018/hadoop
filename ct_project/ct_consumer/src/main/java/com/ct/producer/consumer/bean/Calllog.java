package com.ct.producer.consumer.bean;


import com.ct.common.annotation.Column;
import com.ct.common.annotation.Rowkey;
import com.ct.common.annotation.TableRef;

/**
 * 通话日志对象
 */
@TableRef("ct:calllog")
public class Calllog {
    @Rowkey
    private String rowkey;

    @Column(family = "caller",column = "call1")
    private String call1;

    @Column(family = "caller",column = "call2")
    private String call2;

    @Column(family = "caller",column = "calltime")
    private String calltime;

    @Column(family = "caller",column = "duration")
    private String duration;

    @Column(family = "caller",column = "flag")
    private String flag = "1";

    public Calllog() {
    }

    public Calllog(String data) {
        String[] values = data.split("\t");
        this.call1 = values[0];
        this.call2 = values[1];
        this.calltime = values[2];
        this.duration = values[3];
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
