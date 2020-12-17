package com.sapozhnikov.flights.mapreduce.reducesidejoin.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirlineKey implements WritableComparable<AirlineKey> {

    private Text codeIATA;
    private IntWritable recordTypeId;


    public static final IntWritable AIRLINE_RECORD = new IntWritable(0);
    public static final IntWritable FLIGHT_RECORD = new IntWritable(1);

    public AirlineKey() {
    }

    public AirlineKey(Text codeIATA, IntWritable recordTypeId) {
        this.codeIATA = codeIATA;
        this.recordTypeId = recordTypeId;
    }

    public Text getCodeIATA() {
        return codeIATA;
    }

    public void setCodeIATA(Text codeIATA) {
        this.codeIATA = codeIATA;
    }

    public IntWritable getRecordTypeId() {
        return recordTypeId;
    }

    public void setRecordTypeId(IntWritable recordTypeId) {
        this.recordTypeId = recordTypeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AirlineKey that = (AirlineKey) o;
        return codeIATA.equals(that.codeIATA) &&
                recordTypeId.equals(that.recordTypeId);
    }


    @Override
    public int hashCode() {
        return  codeIATA.hashCode();
    }

    public int compareTo(AirlineKey airlineKey) {
        if (codeIATA.equals(airlineKey.codeIATA)) {
            return recordTypeId.compareTo(airlineKey.recordTypeId);
        }
        return codeIATA.compareTo(airlineKey.codeIATA);
    }

    public void write(DataOutput dataOutput) throws IOException {
        codeIATA.write(dataOutput);
        recordTypeId.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        codeIATA.readFields(dataInput);
        recordTypeId.readFields(dataInput);
    }
}
