package com.hust.ali.jstorm.example.drpc;

import java.util.Map;

import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;

/**
 * Created by zx on 2016/4/20.
 */

public class TestReachTopology {

    /**
     * @param args
     * @throws DRPCExecutionException
     * @throws TException
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("Invalid parameter");
        }
        Map conf = Utils.readStormConfig();
        //"foo.com/blog/1" "engineering.twitter.com/blog/5"
        DRPCClient client = new DRPCClient(conf, args[0], 4772);
        String result = client.execute(ReachTopology.TOPOLOGY_NAME, "tech.backtype.com/blog/123");

        System.out.println("\n!!! Drpc result:" + result);
    }

}
