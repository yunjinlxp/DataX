package com.alibaba.datax.plugin.reader.otsreader.model;


import com.alicloud.openservices.tablestore.model.PrimaryKey;

public class OTSRange {
    
    private PrimaryKey begin = null;
    private PrimaryKey end = null;
    
    public OTSRange() {}
    
    public OTSRange(PrimaryKey begin, PrimaryKey end) {
        this.begin = begin;
        this.end = end;
    }
    
    public PrimaryKey getBegin() {
        return begin;
    }
    public void setBegin(PrimaryKey begin) {
        this.begin = begin;
    }
    public PrimaryKey getEnd() {
        return end;
    }
    public void setEnd(PrimaryKey end) {
        this.end = end;
    }
}
