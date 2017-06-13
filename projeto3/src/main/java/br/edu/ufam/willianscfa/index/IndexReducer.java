package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.ArrayListWritable;
import br.edu.ufam.willianscfa.utils.PairOfStringInt;
import br.edu.ufam.willianscfa.utils.PairOfVInts;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexReducer extends Reducer<PairOfStringInt, VIntWritable, Text, ArrayListWritable<PairOfVInts>> {
    private static ArrayListWritable<PairOfVInts> POSTINGS_LIST = new ArrayListWritable<>();
    private static String prev_term;
    private static final Text KEY = new Text();

    @Override
    public void setup(Context context){
        prev_term = null;
    }

    @Override
    public void reduce(PairOfStringInt term_docid, Iterable<VIntWritable> postings, Context context)
            throws IOException, InterruptedException{

        if(prev_term != null && !term_docid.getLeftElement().equals(prev_term)){
            emit(context);
        }

        // stream de VINTS: t   [(d_1, o_1), (d_2, o_2), (d_3, o_3) ... (d_n, o_n)
        // onde d_i eh o docid do documento i e o_i eh a quantidade de ocorrencias de t (df) no documento i
        // supondo que essa porra desse ArrayListWritable serializa direito.
        // eh tb garantido que esse map eh chamado apenas uma vez para cada par,
        // por isso iterator.next eh suficiente
        POSTINGS_LIST.add(new PairOfVInts(term_docid.getRightElement(), postings.iterator().next().get()));

        prev_term = term_docid.getLeftElement();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        emit(context);
    }


    private static void emit(Context context) throws IOException, InterruptedException{
        if(POSTINGS_LIST.size() == 0){
            return;
        }

        int termoAtual;
        int termoAnterior = 0;
        // d-gaps, shuffle & sort garante que o anterir eh menor que o atual, sem numeros negativos
        for(int i = 0; i < POSTINGS_LIST.size(); i++) {
            termoAtual = POSTINGS_LIST.get(i).getLeftElement(); // salva o cabra na posicao i
            POSTINGS_LIST.get(i).setLeftElement(termoAtual - termoAnterior); // subtrai o cabra atual da posicao i-1 (0 na primeira passada)
            termoAnterior = termoAtual;
        }

        KEY.set(prev_term);
        context.write(KEY, POSTINGS_LIST);

        // P.reset
        POSTINGS_LIST.clear();
    }

}
