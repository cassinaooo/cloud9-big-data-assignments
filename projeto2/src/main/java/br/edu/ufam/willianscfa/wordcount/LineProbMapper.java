package br.edu.ufam.willianscfa.wordcount;

import br.edu.ufam.willianscfa.utils.Tokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by cassiano on 22/05/17.
 */
public class LineProbMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, Integer> counts;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        counts = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        for (String word : Tokenizer.tokenize(value.toString())) {
            if (counts.containsKey(word)) {
                counts.put(word, counts.get(word) + 1);
            } else {
                counts.put(word, 1);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable cnt = new IntWritable();
        Text token = new Text();

        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            token.set(entry.getKey());
            cnt.set(entry.getValue());
            context.write(token, cnt);
        }
    }
}