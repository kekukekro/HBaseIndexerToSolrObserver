package com.bqjr.bigdata.HBaseObserver.server;

import com.bqjr.bigdata.HBaseObserver.comm.config.ConfigManager;
import com.bqjr.bigdata.HBaseObserver.comm.config.HBaseIndexerMappin;
import com.bqjr.bigdata.HBaseObserver.comm.log.LogManager;
import com.bqjr.bigdata.HBaseObserver.entity.SolrCommitTimer;
import com.bqjr.bigdata.HBaseObserver.entity.SolrServerManager;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by hp on 2017-02-15.
 */
public class HBaseIndexerToSolrObserver extends BaseRegionObserver implements LogManager{

    Map<String,List<HBaseIndexerMappin>> mappins = ConfigManager.getHBaseIndexerMappin();

    Timer timer = new Timer();
    int maxCommitTime = ConfigManager.config.getInt("MaxCommitTime"); //最大提交时间，s
    SolrCommitTimer solrCommit = new SolrCommitTimer();
    public HBaseIndexerToSolrObserver(){
        log.info("Initialization HBaseIndexerToSolrObserver ...");
        for(Map.Entry<String,List<HBaseIndexerMappin>> entry : mappins.entrySet() ){
            List<HBaseIndexerMappin> solrmappin = entry.getValue();
            for(HBaseIndexerMappin map:solrmappin){
                String collection = map.solrConnetion;
                log.info("Create Solr Server connection .The collection is " + collection);
                CloudSolrServer solrserver = SolrServerManager.create(collection);
                solrCommit.addCollecttion(collection,solrserver);
            }
        }
        timer.schedule(solrCommit, 10 * 1000L, maxCommitTime * 1000L);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {
        String table =  e.getEnvironment().getRegion().getTableDesc().getTableName().getNameAsString();
        String rowkey= Bytes.toString(put.getRow());
        SolrInputDocument doc = new SolrInputDocument();
        List<HBaseIndexerMappin> mappin = mappins.get(table);
        for(HBaseIndexerMappin mapp : mappin){
            for(String column : mapp.columns){
                String[] tmp = column.split(":");
                String cf = tmp[0];
                String cq = tmp[1];
                if(put.has(Bytes.toBytes(cf),Bytes.toBytes(cq))){
                    Cell cell = put.get(Bytes.toBytes(cf),Bytes.toBytes(cq)).get(0);
                    Map<String, String > operation = new HashMap<String,String>();
                    operation.put("set",Bytes.toString(CellUtil.cloneValue(cell)));
                    doc.setField(cq,operation);
                }
            }
            doc.addField("id",rowkey);
            try {
                solrCommit.addPutDocToCache(mapp.solrConnetion,doc);
            } catch (SolrServerException e1) {
                e1.printStackTrace();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                           Delete delete,
                           WALEdit edit,
                           Durability durability) throws IOException{
        String table =  e.getEnvironment().getRegion().getTableDesc().getTableName().getNameAsString();
        String rowkey= Bytes.toString(delete.getRow());
        List<HBaseIndexerMappin> mappin = mappins.get(table);
        for(HBaseIndexerMappin mapp : mappin){
            try {
                solrCommit.addDeleteIdCache(mapp.solrConnetion,rowkey);
            } catch (SolrServerException e1) {
                e1.printStackTrace();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }

    }


}
