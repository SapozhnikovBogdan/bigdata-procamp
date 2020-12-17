package com.sapozhnikov.flights.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    enum  FlightsRecords {
        RECORD_COUNT, RECORD_MISSED
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        String codeIATA = recordFields[4];
        int departureDelay = 0;
        try {
            departureDelay = Integer.parseInt(recordFields[11]);
        }
        catch (NumberFormatException e) {
            context.getCounter(FlightsRecords.RECORD_MISSED).increment(1);
            return;
        }
        context.write(new Text(codeIATA), new IntWritable(departureDelay));
        context.getCounter(FlightsRecords.RECORD_COUNT).increment(1);
    }

}