package com.hust.ali.jstorm.example.sequence.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import backtype.storm.utils.TupleHelpers;
import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.AsmMeter;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.codahale.metrics.Gauge;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.hust.ali.jstorm.example.TpsCounter;
import com.hust.ali.jstorm.example.sequence.SequenceTopologyDef;
import com.hust.ali.jstorm.example.sequence.bean.TradeCustomer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zx on 2016/4/20.
 */

public class TotalCount implements IRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TotalCount.class);

    private OutputCollector collector;
    private TpsCounter tpsCounter;
    private long lastTupleId = -1;

    private boolean checkTupleId = false;
    private boolean slowDonw = false;

    private MetricClient metricClient;
    private AsmCounter myCounter;
    private AsmMeter myMeter;
    private AsmHistogram myJStormHistogram;
    private AsmGauge myGauge;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());

        checkTupleId = JStormUtils.parseBoolean(stormConf.get("bolt.check.tupleId"), false);
        slowDonw = JStormUtils.parseBoolean(stormConf.get("bolt.slow.down"), false);

        metricClient = new MetricClient(context);

        Gauge<Double> gauge = new Gauge<Double>() {
            private Random random = new Random();

            public Double getValue() {
                return random.nextDouble();
            }

        };
        myGauge = metricClient.registerGauge("myGauge", gauge);
        myCounter = metricClient.registerCounter("myCounter");
        myMeter = metricClient.registerMeter("myMeter");
        myJStormHistogram = metricClient.registerHistogram("myHistogram");

        LOG.info("Finished preparation " + stormConf);
    }

    private AtomicLong tradeSum = new AtomicLong(0);
    private AtomicLong customerSum = new AtomicLong(1);

    public void execute(Tuple input) {

        if (TupleHelpers.isTickTuple(input)) {
            LOG.info("Receive one Ticket Tuple " + input.getSourceComponent());
            return;
        }
        if (input.getSourceStreamId().equals(SequenceTopologyDef.CONTROL_STREAM_ID)) {
            String str = (input.getStringByField("CONTROL"));
            LOG.warn(str);
            return;
        }

        long before = System.currentTimeMillis();
        try {
            myCounter.update(1);
            myMeter.update(1);

            if (checkTupleId) {
                Long tupleId = input.getLong(0);
                if (tupleId <= lastTupleId) {
                    LOG.error("LastTupleId is " + lastTupleId + ", but now:" + tupleId);
                }
                lastTupleId = tupleId;
            }

            TradeCustomer tradeCustomer;
            try {
                tradeCustomer = (TradeCustomer) input.getValue(1);
            } catch (Exception e) {
                LOG.error(input.getSourceComponent() + "  " + input.getSourceTask() + " " + input.getSourceStreamId() + " target " + ((TupleImplExt) input));
                throw new RuntimeException(e);
            }

            tradeSum.addAndGet(tradeCustomer.getTrade().getValue());
            customerSum.addAndGet(tradeCustomer.getCustomer().getValue());

            collector.ack(input);

            long now = System.currentTimeMillis();
            long spend = now - tradeCustomer.getTimestamp();

            tpsCounter.count(spend);
            myJStormHistogram.update(now - before);

            if (slowDonw) {
                JStormUtils.sleepMs(20);
            }
        } finally {
        }

    }

    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info("tradeSum:" + tradeSum + ",cumsterSum" + customerSum);
        LOG.info("Finish cleanup");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
