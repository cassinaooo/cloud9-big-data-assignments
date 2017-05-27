package br.edu.ufam.willianscfa.pmi.stripes;

import br.edu.ufam.willianscfa.utils.HMapStFW;
import br.edu.ufam.willianscfa.utils.PairOfStrings;
import br.edu.ufam.willianscfa.utils.Tokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class StripesMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {

    private final static Text TERMO = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, HMapStFW> stripes = new HashMap<>();
        String[] linha = Tokenizer.tokenize(value.toString());

        if(linha.length < 2) return;

        String palavraEsquerda, palavraDireita;

        for (int i = 0; i < linha.length; i++) {
            for (int j = i + 1; j < linha.length; j++) {
                palavraEsquerda = linha[i];
                palavraDireita = linha[j];

                if (stripes.containsKey(palavraEsquerda)) {
                    HMapStFW stripe = stripes.get(palavraEsquerda);
                    if (stripe.containsKey(palavraDireita)) {
                        stripe.put(palavraDireita, stripe.get(palavraDireita) + 1.0f);
                    } else {
                        stripe.put(palavraDireita, 1.0f);
                    }
                } else {
                    HMapStFW stripe = new HMapStFW();
                    stripe.put(palavraDireita, 1.0f);
                    stripes.put(palavraEsquerda, stripe);
                }
            }
        }

        for (String t : stripes.keySet()) {
            TERMO.set(t);
            context.write(TERMO, stripes.get(t));
        }
    }
}