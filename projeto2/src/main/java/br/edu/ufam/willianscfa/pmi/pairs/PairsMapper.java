package br.edu.ufam.willianscfa.pmi.pairs;

import br.edu.ufam.willianscfa.utils.PairOfStrings;
import br.edu.ufam.willianscfa.utils.Tokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public final class PairsMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

    private final static PairOfStrings PAR = new PairOfStrings();
    private final static IntWritable UM = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] linha = Tokenizer.tokenize(value.toString());

        String palavraEsquerda, palavraDireita;

        for (int i = 0; i < linha.length; i++) {
            for (int j = i + 1; j < linha.length; j++) {
                palavraEsquerda = linha[i];
                palavraDireita = linha[j];

                PAR.set(palavraEsquerda,palavraDireita);
                context.write(PAR, UM);
            }
        }
    }
}