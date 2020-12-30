package com.sapozhnikov.flights.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNAirlainesReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
    final private static int TOP_N_RECORDS = 5;
    private static TreeMap<Double, String> topNTreeMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topNTreeMap = new TreeMap<Double, String>();
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double avgDelay = 0.0;
        for (DoubleWritable value: values){
            avgDelay = value.get();
        }
        topNTreeMap.put(avgDelay, key.toString());

        if (topNTreeMap.size() > TOP_N_RECORDS){
            topNTreeMap.remove(topNTreeMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String output = "";
        /*for (Map.Entry<Double, String> entry: topNTreeMap.entrySet()){
            output = entry.getValue() + ", " + entry.getKey();
            context.write(NullWritable.get(), new Text(output));
        }*/;
        for(double key : topNTreeMap.descendingKeySet()){
            output = String.format("%s, %.2f", topNTreeMap.get(key), key);
            // GLC: No much sense as there is a common counter
            // GLC: It's better to reuse writables
            context.write(NullWritable.get(), new Text(output));
        }
    }
}
