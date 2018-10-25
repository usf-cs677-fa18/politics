package edu.usfca.cs.mr.reddit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Container for the counts of obama, hillary, and trump found in comments.
 * Demonstrates how to make a custom writable, but note that this shouldn't be
 * used as a key because it is *not* comparable. For that, see
 * WritableComparable.
 *
 * @author malensek
 */
public class PoliticsWritable implements Writable {
    private IntWritable month;
    private LongWritable obamaCount;
    private LongWritable hillaryCount;
    private LongWritable trumpCount;

    public PoliticsWritable() {
        this.month = new IntWritable(0);
        this.obamaCount = new LongWritable(0);
        this.hillaryCount = new LongWritable(0);
        this.trumpCount = new LongWritable(0);
    }

    public PoliticsWritable(int month) {
        this();
        this.month = new IntWritable(month);
    }

    public PoliticsWritable(
            IntWritable month,
            LongWritable obamaCount,
            LongWritable hillaryCount,
            LongWritable trumpCount) {

        this.month = month;
        this.obamaCount = obamaCount;
        this.hillaryCount = hillaryCount;
        this.trumpCount = trumpCount;
    }

    public IntWritable getMonth() {
        return month;
    }

    public LongWritable getObamaCount() {
        return obamaCount;
    }

    public LongWritable getHillaryCount() {
        return hillaryCount;
    }

    public LongWritable getTrumpCount() {
        return trumpCount;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public void setObamaCount(LongWritable obamaCount) {
        this.obamaCount = obamaCount;
    }

    public void setHillaryCount(LongWritable hillaryCount) {
        this.hillaryCount = hillaryCount;
    }

    public void setTrumpCount(LongWritable trumpCount) {
        this.trumpCount = trumpCount;
    }

    public void increment(PoliticsWritable pw) {
        incrementObamaCount(pw.obamaCount.get());
        incrementHillaryCount(pw.hillaryCount.get());
        incrementTrumpCount(pw.trumpCount.get());
    }

    public void incrementObamaCount(long count) {
        this.obamaCount.set(this.obamaCount.get() + count);
    }

    private void incrementHillaryCount(long count) {
        this.hillaryCount.set(this.hillaryCount.get() + count);
    }

    public void incrementTrumpCount(long count) {
        this.trumpCount.set(this.trumpCount.get() + count);
    }

    public void incrementObamaCount() {
        this.incrementObamaCount(1);
    }

    public void incrementHillaryCount() {
        this.incrementHillaryCount(1);
    }

    public void incrementTrumpCount() {
        incrementTrumpCount(1);
    }

    public boolean hasCount() {
        return this.obamaCount.get() > 0
                || this.hillaryCount.get() > 0
                || this.trumpCount.get() > 0;
    }

    public void readFields(DataInput in) throws IOException {
        this.month.readFields(in);
        this.obamaCount.readFields(in);
        this.hillaryCount.readFields(in);
        this.trumpCount.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        this.month.write(out);
        this.obamaCount.write(out);
        this.hillaryCount.write(out);
        this.trumpCount.write(out);
    }

    @Override
    public String toString() {
        return obamaCount.toString()
                + "\t" + hillaryCount.toString()
                + "\t" + trumpCount.toString();
    }
}
