package com.bqjr.bigdata.HBaseObserver.entity;

/**
 * Created by hp on 2017-02-16.
 */

import com.bqjr.bigdata.HBaseObserver.comm.config.ConfigManager;
import com.bqjr.bigdata.HBaseObserver.comm.log.LogManager;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * 提交定时器
 */
public class SolrCommitTimer extends TimerTask implements LogManager {
    public Map<String,List<SolrInputDocument>> putCache = new HashMap<String, List<SolrInputDocument>>();
    public Map<String,List<String>> deleteCache = new HashMap<String, List<String>>();
    Map<String,CloudSolrServer> solrServers = new HashMap<String, CloudSolrServer>();
    int maxCache =  ConfigManager.config.getInt("MaxCommitSize");
    // 任何时候，保证只能有一个线程在提交索引，并清空集合
    final static Semaphore semp = new Semaphore(1);
    public void addCollecttion(String collection,CloudSolrServer server){
        this.solrServers.put(collection,server);
    }

    public UpdateResponse put(CloudSolrServer server,SolrInputDocument doc) throws IOException, SolrServerException {
        server.add(doc);
        return server.commit(false, false);
    }

    public UpdateResponse put(CloudSolrServer server,List<SolrInputDocument> docs) throws IOException, SolrServerException {
        server.add(docs);
        return server.commit(false, false);
    }

    public UpdateResponse delete(CloudSolrServer server,String rowkey) throws IOException, SolrServerException {
        server.deleteById(rowkey);
        return server.commit(false, false);
    }

    public UpdateResponse delete(CloudSolrServer server,List<String> rowkeys) throws IOException, SolrServerException {
        server.deleteById(rowkeys);
        return server.commit(false, false);
    }

    public void addPutDocToCache(String collection, SolrInputDocument doc) throws IOException, SolrServerException, InterruptedException {
        semp.acquire();
        log.debug("addPutDocToCache:" + "collection=" + collection + "data=" + doc.toString());
        if(!putCache.containsKey(collection)){
            List<SolrInputDocument> docs = new LinkedList<SolrInputDocument>();
            docs.add(doc);
            putCache.put(collection,docs);
        }else {
            List<SolrInputDocument> cache = putCache.get(collection);
            cache.add(doc);
            if (cache.size() >= maxCache) {
                try {
                    this.put(solrServers.get(collection), cache);
                } finally {
                    putCache.get(collection).clear();
                }
            }
        }
        semp.release();//释放信号量
    }

    public void addDeleteIdCache(String collection,String rowkey) throws IOException, SolrServerException, InterruptedException {
        semp.acquire();
        log.debug("addDeleteIdCache:" + "collection=" + collection + "rowkey=" + rowkey);
        if(!deleteCache.containsKey(collection)){
            List<String> rowkeys = new LinkedList<String>();
            rowkeys.add(rowkey);
            deleteCache.put(collection,rowkeys);
        }else{
            List<String> cache = deleteCache.get(collection);
            cache.add(rowkey);
            if (cache.size() >= maxCache) {
                try{
                    this.delete(solrServers.get(collection),cache);
                }finally {
                    putCache.get(collection).clear();
                }
            }
        }
        semp.release();//释放信号量
    }

    @Override
    public void run() {
        try {
            semp.acquire();
            log.debug("开始插入....");
            Set<String> collections =  solrServers.keySet();
            for(String collection:collections){
                if(putCache.containsKey(collection) && (!putCache.get(collection).isEmpty()) ){
                    this.put(solrServers.get(collection),putCache.get(collection));
                    putCache.get(collection).clear();
                }
                if(deleteCache.containsKey(collection) && (!deleteCache.get(collection).isEmpty())){
                    this.delete(solrServers.get(collection),deleteCache.get(collection));
                    deleteCache.get(collection).clear();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            log.error("Commit putCache to Solr error!Because :" + e.getMessage());
        }finally {
            semp.release();//释放信号量
        }
    }
}