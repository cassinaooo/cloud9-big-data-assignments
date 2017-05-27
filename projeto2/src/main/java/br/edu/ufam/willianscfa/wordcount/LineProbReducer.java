package br.edu.ufam.willianscfa.wordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by cassiano on 22/05/17.
 */
public class LineProbReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    public static final double TOTAL_LINHAS_DOCUMENTO = 122458.0;
    private static final DoubleWritable PROBABILITY = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // Sum up values.
        Iterator<IntWritable> iter = values.iterator();
        double sum = 0;

        while (iter.hasNext()) {
            sum += iter.next().get();
        }

        PROBABILITY.set(sum / TOTAL_LINHAS_DOCUMENTO);

        context.write(key, PROBABILITY);
    }
}