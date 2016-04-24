package com.hust.alipay.example.batch;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.batch.BatchTopologyBuilder;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;
import java.util.Map;

/**
 * Created by zx on 2016/4/20.
 */
public class SimpleBatchTopology {

    private static String topologyName = "Batch";
    private static Map conf;

    public static TopologyBuilder SetBuilder() {
        // 通过BatchTopologyBuilder来关联是spout和bolt，但是源码中这个topologyName并没有用到
        BatchTopologyBuilder topologyBuilder = new BatchTopologyBuilder(topologyName);

        int spoutParallel = JStormUtils.parseInt(conf.get("topology.spout.parallel"), 1);

        // setSpout还是调用的setBolt, spout是一种特殊的bolt
        BoltDeclarer boltDeclarer = topologyBuilder.setSpout("Spout",
                new SimpleSpout(), spoutParallel);

        int boltParallel = JStormUtils.parseInt(conf.get("topology.bolt.parallel"), 2);
        topologyBuilder.setBolt("Bolt", new SimpleBolt(), boltParallel).shuffleGrouping("Spout");

        return topologyBuilder.getTopologyBuilder();
    }

    public static void SetLocalTopology() throws Exception {
        TopologyBuilder builder = SetBuilder();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());

        System.out.println("build test.");
        Thread.sleep(60000);

        cluster.shutdown();
    }

    public static void SetRemoteTopology() throws AlreadyAliveException,
            InvalidTopologyException, TopologyAssignException {
        TopologyBuilder builder = SetBuilder();
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Please input parameters topology.yaml");
            System.exit(-1);
        }

        conf = LoadConf.LoadYaml(args[0]);
        boolean isLocal = StormConfig.local_mode(conf);
        if (isLocal) {
            SetLocalTopology();
        } else {
            SetRemoteTopology();
        }
    }
}
