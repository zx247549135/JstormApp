package com.hust.ali.jstorm.example.sequence.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

import com.hust.ali.jstorm.example.sequence.SequenceTopologyDef;
import com.hust.ali.jstorm.example.TpsCounter;
import com.hust.ali.jstorm.example.sequence.bean.Pair;
import com.hust.ali.jstorm.example.sequence.bean.TradeCustomer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by zx on 2016/4/20.
 */

public class SplitRecord implements IBasicBolt {
    public static Logger LOG = LoggerFactory.getLogger(SplitRecord.class);

    private TpsCounter tpsCounter;

    public void prepare(Map conf, TopologyContext context) {

        tpsCounter = new TpsCounter(context.getThisComponentId() + ":" + context.getThisTaskId());

        LOG.info("Successfully do prepare");
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        tpsCounter.count();

        Long tupleId = tuple.getLong(0);
        Object obj = tuple.getValue(1);

        if (obj instanceof TradeCustomer) {

            TradeCustomer tradeCustomer = (TradeCustomer) obj;

            Pair trade = tradeCustomer.getTrade();
            Pair customer = tradeCustomer.getCustomer();

            collector.emit(SequenceTopologyDef.TRADE_STREAM_ID,
                    new Values(tupleId, trade));

            collector.emit(SequenceTopologyDef.CUSTOMER_STREAM_ID,
                    new Values(tupleId, customer));
        } else if (obj != null) {
            LOG.info("Unknow type " + obj.getClass().getName());
        } else {
            LOG.info("Nullpointer ");
        }
    }


    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info("Finish cleanup");

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SequenceTopologyDef.TRADE_STREAM_ID, new Fields("ID", "TRADE"));
        declarer.declareStream(SequenceTopologyDef.CUSTOMER_STREAM_ID, new Fields("ID", "CUSTOMER"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
