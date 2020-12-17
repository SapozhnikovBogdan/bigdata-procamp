package com.sapozhnikov.flights.mapreduce.reducesidejoin;

import com.sapozhnikov.flights.mapreduce.reducesidejoin.model.AirlineKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlineMapper extends Mapper<LongWritable, Text, AirlineKey, Text> {
    private final String IATA_CODE = "IATA_CODE";
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split(",");
        String codeIATA  = recordFields[0];
        String airline = recordFields[1];

        if (codeIATA == IATA_CODE ) return;

        AirlineKey airlineKey = new AirlineKey(new Text(codeIATA), AirlineKey.AIRLINE_RECORD);
        context.write(airlineKey, new Text(airline));

    }
}
