首先需要添加morphlines.conf文件。里面包含了需要同步数据到Solr的HBase表名、对应的Solr Collection的名字、要同步的列、多久提交一次、最大批次容量的相关信息。具体配置如下：

```conf
#最大提交时间（单位：秒）
MaxCommitTime = 30
#最大批次容量
MaxCommitSize = 10000

Mappin {
  HBaseTables: ["HBASE_OBSERVER_TEST"] #需要同步的HBase表名
  "HBASE_OBSERVER_TEST": [
    {
      SolrCollection: "bqjr" #Solr Collection名字
  Columns: [
        "cf1:test_age",   #需要同步的列，格式<列族:列>
  "cf1:test_name"
  ]
    },
  ]
}
```

该配置文件默认放在各个节点的`/etc/hbase/conf/`下。如果你希望将配置文件路径修改为其他路径，请修改com.bqjr.bigdata.HBaseObserver.comm.config.SourceConfig类中的configHome路径。

然后将代码打包，上传到HDFS中，将协处理器添加到对应的表中。
```shell
#先禁用这张表
disable 'HBASE_OBSERVER_TEST'
#为这张表添加协处理器,设置的参数具体为： jar文件路径|类名|优先级（SYSTEM或者USER）
alter 'HBASE_OBSERVER_TEST','coprocessor'=>'hdfs://hostname:8020/ext_lib/HBaseObserver-1.0.0.jar|com.bqjr.bigdata.HBaseObserver.server.HBaseIndexerToSolrObserver||'
#启用这张表
enable 'HBASE_OBSERVER_TEST'
#删除某个协处理器，"$<bumber>"后面跟的ID号与desc里面的ID号相同
alter 'HBASE_OBSERVER_TEST',METHOD=>'table_att_unset',NAME => 'coprocessor$1'
```

如果需要新增一张表同步到Solr。只需要修改morphlines.conf文件，分发倒各个节点。然后将协处理器添加到HBase表中，这样就不用再次修改代码了。
