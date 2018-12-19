package cn.analysys.tag.entity;

import java.io.Serializable;

public class IPArea implements Serializable {

    private static final long serialVersionUID = -3754624942552513392L;
    private Long startIp = 0L;// IP段开始 原始字段from_number
    private Long endIp = 0L;// IP段结束 原始字段to_number

    public IPArea() {

    }

    public IPArea(long startIp, long endIp) {
        this.startIp = startIp;
        this.endIp = endIp;
    }

    public long getStartIp() {
        return startIp;
    }

    public void setStartIp(long startIp) {
        this.startIp = startIp;
    }

    public long getEndIp() {
        return endIp;
    }

    public void setEndIp(long endIp) {
        this.endIp = endIp;
    }

}
