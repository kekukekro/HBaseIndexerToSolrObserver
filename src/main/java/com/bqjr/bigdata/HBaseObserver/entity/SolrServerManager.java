package com.bqjr.bigdata.HBaseObserver.entity;

import com.bqjr.bigdata.HBaseObserver.comm.log.LogManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
/**
 * Created by hp on 2017-02-15.
 */
public class SolrServerManager implements LogManager {
    static Configuration conf = HBaseConfiguration.create();
    public static String ZKHost = conf.get("hbase.zookeeper.quorum","bqdpm1,bqdpm2,bqdps2");
    public static String ZKPort = conf.get("hbase.zookeeper.property.clientPort","2181");
    public static String SolrUrl = ZKHost + ":" + ZKPort + "/" + "solr";
    public static int zkClientTimeout = 1800000;// 心跳
    public static int zkConnectTimeout = 1800000;// 连接时间

    public static  CloudSolrServer create(String defaultCollection){
        log.info("Create SolrCloudeServer .This collection is " + defaultCollection);
        CloudSolrServer solrServer = new CloudSolrServer(SolrUrl);
        solrServer.setDefaultCollection(defaultCollection);
        solrServer.setZkClientTimeout(zkClientTimeout);
        solrServer.setZkConnectTimeout(zkConnectTimeout);
        return solrServer;
    }
}
