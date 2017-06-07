package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.PairOfStringInt;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexReducer extends Reducer<Text, PairOfStringInt, Text, Text> {
    private static final Text POSTINGS_ARRAY = new Text();

    @Override
    public void reduce(Text term, Iterable<PairOfStringInt> postings, Context context )
            throws IOException, InterruptedException{

        // tendo que emitir Text aqui, mas a saida eh um array de postings da forma: TERM \t [(p1, n1), (p2, n2), (p3, n3)]
        // aonde pi e ni sao, respectivamente, o caminho para um arquivo, e a quantidade de ocorrencias de TERM naquele arquivo

        StringBuilder result = new StringBuilder();
        result.append("[");

        for(PairOfStringInt pair : postings){
            result.append("(").append(pair.getLeftElement()).append(": ").append(pair.getRightElement()).append("), ");
        }

        result.delete(result.length() - 2, result.length());

        result.append("]");

        POSTINGS_ARRAY.set(result.toString());

        context.write(term, POSTINGS_ARRAY);
    }

}
