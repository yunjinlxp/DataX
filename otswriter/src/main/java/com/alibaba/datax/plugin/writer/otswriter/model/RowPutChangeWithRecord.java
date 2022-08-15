package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alibaba.datax.common.element.Record;

public class RowPutChangeWithRecord extends com.alicloud.openservices.tablestore.model.RowPutChange implements WithRecord {

    private Record record;

    public RowPutChangeWithRecord(String tableName) {
        super(tableName);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public void setRecord(Record record) {
        this.record = record;
    }
}
