package com.bqjr.bigdata.HBaseObserver.comm.config;

import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by hp on 2017-02-16.
 */
public class ConfigManager {
    private static SourceConfig sourceConfig = new SourceConfig();
    public static Config config;
    static {
        sourceConfig.setConfigFiles("morphlines.conf");
        config =  sourceConfig.getConfig();
    }
    public static Map<String,List<HBaseIndexerMappin>> getHBaseIndexerMappin(){
        Map<String,List<HBaseIndexerMappin>> mappin = new HashMap<String, List<HBaseIndexerMappin>>();
        Config mappinConf = config.getConfig("Mappin");
        List<String> tables = mappinConf.getStringList("HBaseTables");
        for (String table :tables){
            List<Config> confList = (List<Config>) mappinConf.getConfigList(table);
            List<HBaseIndexerMappin> maps = new LinkedList<HBaseIndexerMappin>();
            for(Config tmp :confList){
                HBaseIndexerMappin map = new HBaseIndexerMappin();
                map.solrConnetion = tmp.getString("SolrCollection");
                map.columns = tmp.getStringList("Columns");
                maps.add(map);
            }
            mappin.put(table,maps);
        }
        return mappin;
    }
}
