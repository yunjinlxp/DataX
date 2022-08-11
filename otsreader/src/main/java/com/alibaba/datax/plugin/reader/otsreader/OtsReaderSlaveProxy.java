package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;
import java.util.concurrent.Future;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetRangeCallable;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsreader.utils.DefaultNoRetry;
import com.alibaba.datax.plugin.reader.otsreader.utils.RetryHelper;

public class OtsReaderSlaveProxy {
    
    class RequestItem {
        private RangeRowQueryCriteria criteria;
        private Future<GetRangeResponse> future;
        
        RequestItem(RangeRowQueryCriteria criteria, Future<GetRangeResponse> future) {
            this.criteria = criteria;
            this.future = future;
        }

        public RangeRowQueryCriteria getCriteria() {
            return criteria;
        }

        public Future<GetRangeResponse> getFuture() {
            return future;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderSlaveProxy.class);
    
    private void rowsToSender(List<Row> rows, RecordSender sender, List<OTSColumn> columns) {
        for (Row row : rows) {
            Record line = sender.createRecord();
            line = Common.parseRowToLine(row, columns, line);
            
            LOG.debug("Reader send record : {}", line.toString());
            
            sender.sendToWriter(line);
        }
    }
    
    private RangeRowQueryCriteria generateRangeRowQueryCriteria(String tableName, PrimaryKey begin, PrimaryKey end, Direction direction, List<String> columns) {
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setDirection(direction);
        criteria.setMaxVersions(Integer.MAX_VALUE);
        // criteria.setcolumns(columns);
        criteria.setLimit(1);
        criteria.setExclusiveEndPrimaryKey(end);
        return criteria;
    }
    
    private RequestItem generateRequestItem(
            AsyncClient ots,
            OTSConf conf, 
            PrimaryKey begin,
            PrimaryKey end,
            Direction direction, 
            List<String> columns) throws Exception {
        RangeRowQueryCriteria criteria = generateRangeRowQueryCriteria(conf.getTableName(), begin, end, direction, columns);
        criteria.setMaxVersions(1);
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        Future<GetRangeResponse> future =  ots.getRange(request, null);
        
        return new RequestItem(criteria, future);
    }

    public void read(RecordSender sender, Configuration configuration) throws Exception {
        LOG.info("read begin.");
        
        OTSConf conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));
        OTSRange range = GsonParser.jsonToRange(configuration.getString(OTSConst.OTS_RANGE));
        Direction direction = GsonParser.jsonToDirection(configuration.getString(OTSConst.OTS_DIRECTION));

        ClientConfiguration configure = new ClientConfiguration();
        configure.setRetryStrategy(new DefaultNoRetry());

        AsyncClient ots = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccesskey(),
                conf.getInstanceName(),
                configure);
        
        PrimaryKey token = range.getBegin();
        List<String> columns = Common.getNormalColumnNameList(conf.getColumns());
        
        RequestItem request = null;
        
        do {
            LOG.debug("Next token : {}", GsonParser.rowPrimaryKeyToJson(token));
            if (request == null) {
                request = generateRequestItem(ots, conf, token, range.getEnd(), direction, columns);
            } else {
                RequestItem req = request;

                GetRangeResponse result = RetryHelper.executeWithRetry(
                        new GetRangeCallable(ots, req.getCriteria(), req.getFuture()),
                        conf.getRetry(),
                        conf.getSleepInMilliSecond()
                    );
                if ((token = result.getNextStartPrimaryKey()) != null) {
                    request = generateRequestItem(ots, conf, token, range.getEnd(), direction, columns);
                }
                
                rowsToSender(result.getRows(), sender, conf.getColumns());
            }
        } while (token != null);
        ots.shutdown();
        LOG.info("read end.");
    }
}
