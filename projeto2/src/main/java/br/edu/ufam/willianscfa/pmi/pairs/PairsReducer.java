package br.edu.ufam.willianscfa.pmi.pairs;

import br.edu.ufam.willianscfa.utils.DirReader;
import br.edu.ufam.willianscfa.utils.PairOfStrings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class PairsReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
    private static Map<String, Double> termTotals = new HashMap<>();

    private static DoubleWritable PMI = new DoubleWritable();
    private static final double TOTAL_LINHAS_DOCUMENTO = 122458.0;

    @Override
    public void setup(Context context) throws IOException {
        termTotals = DirReader.getWordCount(context);
    }

    @Override
    public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context )
            throws IOException, InterruptedException{
        int pairSum = 0;
        
        for(IntWritable value : values) {
            pairSum += value.get();
        }

        if(pairSum >= 10){

            String left = pair.getLeftElement();
            String right = pair.getRightElement();

            double probPair = pairSum / TOTAL_LINHAS_DOCUMENTO;
            double probLeft = termTotals.get(left);
            double probRight = termTotals.get(right);

            double pmi = Math.log10(probPair / (probLeft * probRight));

            pair.set(left, right);

            PMI.set(pmi);

            context.write(pair, PMI);
        }

    }

}
