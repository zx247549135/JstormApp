package com.hust.ali.jstorm.example.sequence.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hust.ali.jstorm.example.sequence.bean.Pair;
import com.hust.ali.jstorm.example.sequence.bean.TradeCustomer;

/**
 * Created by zx on 2016/4/20.
 */

public class TradeCustomerSerializer extends Serializer<TradeCustomer> {

    PairSerializer pairSerializer = new PairSerializer();

    @Override
    public TradeCustomer read(Kryo kryo, Input input, Class<TradeCustomer> arg2) {

        Pair custormer = kryo.readObject(input, Pair.class);
        Pair trade = kryo.readObject(input, Pair.class);

        long timeStamp = input.readLong();
        String buffer = input.readString();

        TradeCustomer inner = new TradeCustomer(timeStamp, trade, custormer, buffer);
        return inner;
    }

    @Override
    public void write(Kryo kryo, Output output, TradeCustomer inner) {

        kryo.writeObject(output, inner.getCustomer());
        kryo.writeObject(output, inner.getTrade());
        output.writeLong(inner.getTimestamp());
        output.writeString(inner.getBuffer());
    }

}
