package com.alibaba.datax.plugin.reader.otsreader.callable;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;


public class GetRangeCallable implements Callable<GetRangeResponse> {
    
    private AsyncClient ots;
    private RangeRowQueryCriteria criteria;
    private Future<GetRangeResponse> future;
    
    public GetRangeCallable(AsyncClient ots, RangeRowQueryCriteria criteria, Future<GetRangeResponse> future) {
        this.ots = ots;
        this.criteria = criteria;
        this.future = future;
    }
    
    @Override
    public GetRangeResponse call() throws Exception {
        try {
            return future.get();
        } catch (Exception e) {
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            future = ots.getRange(request, null);
            throw e;
        }
    }

}
