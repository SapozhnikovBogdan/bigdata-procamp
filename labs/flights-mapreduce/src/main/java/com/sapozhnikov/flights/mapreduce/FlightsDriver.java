package com.sapozhnikov.flights.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static java.lang.String.format;

public class FlightsDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new FlightsDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        //String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        int result;

        if (args.length < 5) {
            System.err.println("Error: please provide 4 parameters : <input folder>  <output_folder> <airline_file_path> <temp output folder>");
            return 2;
        }
        result = this.runJobAgvDelay(args);

        if (result != 0) {
            System.exit(result);
        }

        return this.runJobTopNAirlines(args);
    }

    private int runJobAgvDelay(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputFolder = args[1];
        String tempOutputFolder = args[4];

        Job jobAgvDelay = Job.getInstance(getConf(), "Avg Departure Delay by Airlines");
        jobAgvDelay.setJarByClass(FlightsDriver.class);

        jobAgvDelay.setMapperClass(FlightsMapper.class);
        jobAgvDelay.setReducerClass(FlightsReducer.class);

        jobAgvDelay.setMapOutputKeyClass(Text.class);
        jobAgvDelay.setMapOutputValueClass(IntWritable.class);

        jobAgvDelay.setOutputKeyClass(Text.class);
        jobAgvDelay.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(jobAgvDelay, new Path(inputFolder));

        Path tempOutputPath = new Path(tempOutputFolder);
        FileOutputFormat.setOutputPath(jobAgvDelay, tempOutputPath);
        if (tempOutputPath.getFileSystem(getConf()).delete(tempOutputPath, true)){
            System.out.println(tempOutputPath.getName() + " folder was removed");
        }

        if (!jobAgvDelay.waitForCompletion(true)) {
            return 1;
        }

        Counters jobAgvDelayCounters = jobAgvDelay.getCounters();
        Counter recFltCount =  jobAgvDelayCounters.findCounter(FlightsMapper.FlightsRecords.RECORD_COUNT);
        Counter recFltMissed =  jobAgvDelayCounters.findCounter(FlightsMapper.FlightsRecords.RECORD_MISSED);

        System.out.println(format("%s : %d", recFltCount.getDisplayName(), recFltCount.getValue()));
        System.out.println(format("%s : %d", recFltMissed.getDisplayName(), recFltMissed.getValue()));

        return 0;
    }

    private int runJobTopNAirlines(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String outputFolder = args[2];
        String dictionaryFilePath = args[3];
        String tempOutputFolder = args[4];

        Job jobTopNAirlines = Job.getInstance(getConf(), "Top 5 Airlines with Agv Departure Delay");
        jobTopNAirlines.setJarByClass(FlightsDriver.class);

        jobTopNAirlines.setMapperClass(TopNAirlainesMapper.class);
        jobTopNAirlines.setReducerClass(TopNAirlainesReducer.class);

        jobTopNAirlines.setMapOutputKeyClass(Text.class);
        jobTopNAirlines.setMapOutputValueClass(DoubleWritable.class);

        jobTopNAirlines.setOutputKeyClass(NullWritable.class);
        jobTopNAirlines.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobTopNAirlines, new Path(tempOutputFolder));
        Path outputPath = new Path(outputFolder);
        FileOutputFormat.setOutputPath(jobTopNAirlines, outputPath);
        if (outputPath.getFileSystem(getConf()).delete(outputPath, true)){
            System.out.println(outputPath.getName() + " folder was removed");
        }
        jobTopNAirlines.addCacheFile(new Path(dictionaryFilePath).toUri());

        jobTopNAirlines.setNumReduceTasks(1);

        if (!jobTopNAirlines.waitForCompletion(true)) {
            return 1;
        }

        Counters jobTopNAirlineCounters = jobTopNAirlines.getCounters();
        Counter fileNotFound =  jobTopNAirlineCounters.findCounter(TopNAirlainesMapper.AirlinesFileStatus.FILE_NOT_FOUND);
        Counter ioError =  jobTopNAirlineCounters.findCounter(TopNAirlainesMapper.AirlinesFileStatus.IO_ERROR);
        Counter recCount =  jobTopNAirlineCounters.findCounter(TopNAirlainesMapper.AirlinesRecords.RECORD_COUNT);
        Counter recMissed =  jobTopNAirlineCounters.findCounter(TopNAirlainesMapper.AirlinesRecords.RECORD_MISSED);

        System.out.println(format("%s : %d", fileNotFound.getDisplayName(), fileNotFound.getValue()));
        System.out.println(format("%s : %d", ioError.getDisplayName(), ioError.getValue()));
        System.out.println(format("%s : %d", recCount.getDisplayName(), recCount.getValue()));
        System.out.println(format("%s : %d", recMissed.getDisplayName(), recMissed.getValue()));

        return 0;
    }

}
