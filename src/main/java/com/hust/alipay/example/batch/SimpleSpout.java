package com.hust.alipay.example.batch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.IBatchSpout;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zx on 2016/4/20.
 */

public class SimpleSpout implements IBatchSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSpout.class);
    private Random rand;
    private int batchSize = 100;

    public void prepare(Map stormConf, TopologyContext context) {
        rand = new Random();
        rand.setSeed(System.currentTimeMillis());
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("Test: This spout is executing!");
        BatchId batchId = (BatchId) input.getValue(0);

        for (int i = 0; i < batchSize; i++) {
            long value = rand.nextInt(10);
            collector.emit(new Values(batchId, value));
        }
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("BatchId", "Value"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public byte[] commit(BatchId id) throws FailedException {
        LOG.info("Receive BatchId " + id);
        return null;
    }

    public void revert(BatchId id, byte[] commitResult) {
        LOG.info("Receive BatchId " + id);
    }

}