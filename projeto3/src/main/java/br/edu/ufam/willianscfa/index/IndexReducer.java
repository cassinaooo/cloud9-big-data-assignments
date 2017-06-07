package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexReducer extends Reducer<PairOfStrings, VIntWritable, Text, Text> {
    private static final StringBuilder postings_list = new StringBuilder();
    private static String prev_term;

    private static final Text KEY = new Text(), VALUE = new Text();

    @Override
    public void setup(Context context){
        prev_term = null;
    }

    @Override
    public void reduce(PairOfStrings term_docid, Iterable<VIntWritable> postings, Context context)
            throws IOException, InterruptedException{

        // tendo que emitir Text aqui, mas a saida eh um array de postings da forma: TERM \t [(p1, n1), (p2, n2), (p3, n3)]
        // aonde pi e ni sao, respectivamente, o caminho para um arquivo, e a quantidade de ocorrencias de TERM naquele arquivo

        if(prev_term != null && !term_docid.getLeftElement().equals(prev_term)){
            // apaga um espaco e a virgula no final
            postings_list.delete(postings_list.length() - 2,postings_list.length());
            // formatacao
            postings_list.append("]");

            KEY.set(prev_term);
            VALUE.set(postings_list.toString());

            context.write(KEY, VALUE);

            // P.reset
            postings_list.setLength(0);
            postings_list.append("[");
        }

        postings_list.append("(").append(term_docid.getRightElement()).append(": ").append(postings.iterator().next()).append(" ), ");
        prev_term = term_docid.getLeftElement();

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        // apaga um espaco e a virgula no final
        postings_list.delete(postings_list.length() - 2,postings_list.length());
        // formatacao
        postings_list.append("]");
        if(prev_term != null){
            KEY.set(prev_term);
            VALUE.set(postings_list.toString());

            context.write(KEY, VALUE);
        }
    }

}
