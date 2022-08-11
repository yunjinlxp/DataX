package com.alibaba.datax.plugin.reader.otsreader.callable;

import java.util.concurrent.Callable;

import com.alicloud.openservices.tablestore.InternalClient;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;

public class GetTableMetaCallable implements Callable<TableMeta>{

    private InternalClient ots = null;
    private String tableName = null;
    
    public GetTableMetaCallable(InternalClient ots, String tableName) {
        this.ots = ots;
        this.tableName = tableName;
    }
    
    @Override
    public TableMeta call() throws Exception {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        DescribeTableResponse result = ots.describeTable(describeTableRequest, null).get();
        TableMeta tableMeta = result.getTableMeta();
        return tableMeta;
    }

}
