package edu.usfca.cs.mr.reddit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;

/**
 * Container for year-month pairs, output in the format YYYY-MM. This class
 * demonstrates how to make a WritableComparable suitable for use as a *key* or
 * value.
 *
 * IMPORTANT: You need to implement hashCode() for Hadoop to partition keys
 * correctly!
 *
 * @author malensek
 */
public class DateWritable implements WritableComparable<DateWritable> {

    private IntWritable year = new IntWritable(0);
    private IntWritable month = new IntWritable(0);

    public DateWritable() { }

    public DateWritable(int year, int month) {
        this.year = new IntWritable(year);
        this.month = new IntWritable(month);
    }

    public DateWritable(LocalDate date) {
        this(date.getYear(), date.getMonthValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateWritable that = (DateWritable) o;

        if (year != null ? !year.equals(that.year) : that.year != null)
            return false;
        return month != null ? month.equals(that.month) : that.month == null;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (month != null ? month.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(DateWritable that) {
        if (that.year != this.year) {
            return that.year.compareTo(this.year);
        }

        return that.month.compareTo(this.month);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.month.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.month.readFields(dataInput);
    }

    @Override
    public String toString() {
        return year.toString() + "-" + String.format("%02d", month.get());
    }
}
