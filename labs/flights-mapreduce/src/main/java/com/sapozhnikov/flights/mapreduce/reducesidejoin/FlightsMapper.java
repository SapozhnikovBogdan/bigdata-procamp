package com.sapozhnikov.flights.mapreduce.reducesidejoin;

import com.sapozhnikov.flights.mapreduce.reducesidejoin.model.AirlineKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, AirlineKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        String codeIATA = recordFields[4];
        String departureDelayStr = recordFields[4];
        try {
          int  departureDelay = Integer.parseInt(recordFields[0]);
        }
        catch (NumberFormatException e) {
            return;
        }

        AirlineKey airlineKey = new AirlineKey(new Text(codeIATA), AirlineKey.FLIGHT_RECORD);
        context.write(airlineKey, new Text(departureDelayStr));

    }

}

