package edu.usfca.cs.mr.reddit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Collects the subtotals from the Map stage and combines them for the final
 * per-month totals.
 *
 * @author malensek
 */
public class PoliticsReducer
extends Reducer<DateWritable, PoliticsWritable, DateWritable, PoliticsWritable> {

    @Override
    protected void reduce(
            DateWritable key, Iterable<PoliticsWritable> values, Context context)
    throws IOException, InterruptedException {

        PoliticsWritable pw = new PoliticsWritable();

        for(PoliticsWritable val : values){
            pw.increment(val);
        }

        context.write(key, pw);
    }

}
