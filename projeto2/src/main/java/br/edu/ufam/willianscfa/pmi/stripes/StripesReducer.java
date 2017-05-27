package br.edu.ufam.willianscfa.pmi.stripes;

import br.edu.ufam.willianscfa.utils.DirReader;
import br.edu.ufam.willianscfa.utils.HMapStFW;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class StripesReducer extends Reducer<Text, HMapStFW, Text, HMapStFW>  {
    private static Map<String, Double> termTotals;
    private static final float TOTAL_LINHAS_DOCUMENTO = 122458.0f;

    @Override
    public void setup(Context context) throws IOException {
        termTotals = DirReader.getWordCount(context);
    }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
            throws IOException, InterruptedException {
        Iterator<HMapStFW> iter = values.iterator();

        HMapStFW map = new HMapStFW();

        while (iter.hasNext()) {
            map.plus(iter.next());
        }

        HMapStFW pmi_map = new HMapStFW();

        double probLeft = termTotals.get(key.toString()).floatValue();

        for (String term : map.keySet()) {
            if(map.get(term) >= 10){
                float probPair = map.get(term) / TOTAL_LINHAS_DOCUMENTO;
                float probRight = termTotals.get(term).floatValue();

                float pmi = (float) Math.log10(probPair / (probLeft * probRight));

                pmi_map.put(term, pmi);
            }

        }
        // as vezes nao tem ninguem com mais de 10 ocorrencias
        if(pmi_map.length() > 0){
            context.write(key, pmi_map);
        }
    }
}


