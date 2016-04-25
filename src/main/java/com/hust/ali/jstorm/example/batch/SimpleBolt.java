package com.hust.ali.jstorm.example.batch;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.ICommitter;
import com.alibaba.jstorm.utils.JStormUtils;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zx on 2016/4/20.
 */

public class SimpleBolt implements IBasicBolt, ICommitter {
    private static final long serialVersionUID = 5720810158625748042L;
    private static final Logger LOG = LoggerFactory.getLogger(SimpleBolt.class);

    public static final String COUNT_BOLT_NAME = "Count";
    public static final String SUM_BOLT_NAME = "Sum";
    private Map conf;

    private TimeCacheMap<BatchId, AtomicLong> counters;

    public void prepare(Map stormConf, TopologyContext context) {
        this.conf = stormConf;

        int timeoutSeconds = JStormUtils.parseInt(
                conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
        counters = new TimeCacheMap<BatchId, AtomicLong>(timeoutSeconds);

        LOG.info("Successfully do prepare");
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("Test: This bolt is executing!");
        BatchId id = (BatchId) input.getValue(0);
        Long value = input.getLong(1);

        AtomicLong counter = counters.get(id);
        if (counter == null) {
            counter = new AtomicLong(0);
            counters.put(id, counter);
        }
        counter.addAndGet(value);
    }

    public void cleanup() {
        LOG.info("Successfully do cleanup");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("BatchId", "counters"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private BatchId currentId;

    public byte[] commit(BatchId id) throws FailedException {
        LOG.info("Receive BatchId " + id);
        if (currentId == null) {
            currentId = id;
        } else if (currentId.getId() >= id.getId()) {
            LOG.info("Current BatchId is " + currentId + ", receive:"
                    + id);
            throw new RuntimeException();
        }
        currentId = id;

        AtomicLong counter = (AtomicLong) counters.remove(id);
        if (counter == null) {
            counter = new AtomicLong(0);
        }

        LOG.info("Flush " + id + "," + counter);
        return Utils.serialize(id);
    }

    public void revert(BatchId id, byte[] commitResult) {
        LOG.info("Receive BatchId " + id);

        BatchId failedId = (BatchId) Utils.javaDeserialize(commitResult);

        if (!failedId.equals(id)) {
            LOG.info("Deserialized error  " + id);
        }
    }
}
