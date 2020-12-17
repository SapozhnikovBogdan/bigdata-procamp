package com.sapozhnikov.flights.mapreduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TopNAirlainesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    final private static int TOP_N_RECORDS = 5;
    private static HashMap<String, String> airlinesHashMap = new HashMap<String, String>();
    private static TreeMap<Double, String>  topNTreeMap;

    enum  AirlinesFileStatus {
        FILE_NOT_FOUND, IO_ERROR
    }

    enum  AirlinesRecords {
        RECORD_COUNT, RECORD_MISSED
    }

    private void loadAirlinesHashMap(String filePath, Context context) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(filePath);
        try (BufferedReader brReader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String strLineRead = "";
            // Read each line, split and load to HashMap
            while ((strLineRead = brReader.readLine()) != null) {
                String AirlineArray[] = strLineRead.split(",");
                airlinesHashMap.put(AirlineArray[0].trim(), AirlineArray[1].trim());
            }
        } catch (FileNotFoundException e) {
            context.getCounter(AirlinesFileStatus.FILE_NOT_FOUND).increment(1);
            e.printStackTrace();
        } catch (IOException e) {
            context.getCounter(AirlinesFileStatus.IO_ERROR).increment(1);
            e.printStackTrace();
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            for(URI uri : cacheFiles)
                loadAirlinesHashMap(uri.getPath(), context);
        }
        topNTreeMap = new TreeMap<Double, String>();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split("\\t");
        String codeIATA = recordFields[0];
        String airlineName = "";
        Double avgDepDelay = 0.0;

        try {
            airlineName = airlinesHashMap.get(codeIATA);;
        } finally {
            airlineName = ((airlineName.equals(null) || airlineName.equals("")) ? "NOT-FOUND" : airlineName);
        }

        try {
            avgDepDelay = Double.parseDouble(recordFields[1]);
        }
        catch (NumberFormatException | NullPointerException e) {
            context.getCounter(AirlinesRecords.RECORD_MISSED).increment(1);
            return;
        }

        topNTreeMap.put(avgDepDelay, codeIATA + ", " + airlineName);

        //left only first TOP_N_RECORDS
        if (topNTreeMap.size() > TOP_N_RECORDS) {
            topNTreeMap.remove(topNTreeMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double, String> entry: topNTreeMap.entrySet())
        {
            double avgDelay = entry.getKey();
            String airline_code_name = entry.getValue();
            context.write(new Text(airline_code_name), new DoubleWritable(avgDelay));
            context.getCounter(AirlinesRecords.RECORD_COUNT).increment(1);
        }
    }
}
