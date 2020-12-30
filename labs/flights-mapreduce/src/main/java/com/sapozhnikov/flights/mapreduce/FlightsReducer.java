package com.sapozhnikov.flights.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    // GLC| if you set the only reducer for the job
    // GLC| you can implement TopN algorithm right in here and write on Reducer.cleanup
    // GLC| so there is no need for extra MR jobs
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double totalDepartureDelay = 0.0;
        long count= 0;
        for (IntWritable value :  values){
          totalDepartureDelay += value.get();
          count++;
        }
        // GLC: It's better to reuse writables
        context.write(key, new DoubleWritable(totalDepartureDelay/count));
    }
}
