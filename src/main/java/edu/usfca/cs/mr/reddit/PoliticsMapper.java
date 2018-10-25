package edu.usfca.cs.mr.reddit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import java.util.StringTokenizer;

/**
 * Reads comments in JSON format, parses the comment body and date, and then
 * emits the number of 'obama', 'hillary', and 'trump' found.
 * - Key: comment year/month, YYYY-MM
 * - Value: counts represented as a PoliticsWritable
 *
 * @author malensek
 */
public class PoliticsMapper
extends Mapper<LongWritable, Text, DateWritable, PoliticsWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        JSONObject obj = new JSONObject(value.toString());

        String sub = obj.getString("subreddit");
        if (sub.equals("politics") == false) {
            return;
        }

        /* Let's find the month that the comment was created on. We can also
         * find this information by inspecting the data file name */
        long dateTimestamp = obj.getLong("created_utc");
        LocalDate date= Instant
                .ofEpochSecond(dateTimestamp)
                .atZone(ZoneId.of("UTC"))
                .toLocalDate();
        int month = date.getMonthValue();

        /* Comment body text: */
        String commentText = obj.getString("body");
        PoliticsWritable pw = new PoliticsWritable(month);

        /* Tokenize the comment into words. */
        StringTokenizer itr = new StringTokenizer(commentText);

        while (itr.hasMoreTokens()) {
            /* For each token, check for occurrences of 'obama', 'hillary',
             * and 'trump'. */
            String token = itr.nextToken().toLowerCase();

            if (token.equals("obama")) {
                pw.incrementObamaCount();
            }

            if (token.equals("hillary")) {
                pw.incrementHillaryCount();
            }

            if (token.equals("trump")) {
                pw.incrementTrumpCount();
            }
        }

        if (pw.hasCount() == true) {
            //System.out.println(pw);
            /* We will only write something to the output if at least one of
             * the names above was present */
            context.write(new DateWritable(date), pw);
        }
    }
}
