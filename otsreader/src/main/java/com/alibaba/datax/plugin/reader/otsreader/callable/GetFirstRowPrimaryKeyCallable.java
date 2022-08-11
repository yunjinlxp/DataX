package com.alibaba.datax.plugin.reader.otsreader.callable;

import com.alicloud.openservices.tablestore.InternalClient;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class GetFirstRowPrimaryKeyCallable implements Callable<PrimaryKey> {

    private InternalClient ots = null;
    private TableMeta meta = null;
    private RangeRowQueryCriteria criteria = null;

    public GetFirstRowPrimaryKeyCallable(InternalClient ots, TableMeta meta, RangeRowQueryCriteria criteria) {
        this.ots = ots;
        this.meta = meta;
        this.criteria = criteria;
    }

    @Override
    public PrimaryKey call() throws Exception {

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        GetRangeResponse result = ots.getRange(request, null).get();
        List<Row> rows = result.getRows();
        if (rows.isEmpty()) {
            return null;// no data
        }
        Row row = rows.get(0);

        Map<String, PrimaryKeyType> pk = meta.getPrimaryKeyMap();
        List<PrimaryKeyColumn> primaryKeys = new ArrayList<>();
        for (String key : pk.keySet()) {
            PrimaryKeyColumn primaryKeyColumn = row.getPrimaryKey().getPrimaryKeyColumnsMap().get(key);
            primaryKeys.add(primaryKeyColumn);
        }
        return new PrimaryKey(primaryKeys);
    }

}
