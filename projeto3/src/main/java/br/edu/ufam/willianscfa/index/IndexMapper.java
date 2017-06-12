package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.FileIdExtractor;
import br.edu.ufam.willianscfa.utils.PairOfStringInt;
import br.edu.ufam.willianscfa.utils.Tokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public final class IndexMapper extends Mapper<Text, Text, PairOfStringInt, VIntWritable> {
    private static final PairOfStringInt TERM_DOCID = new PairOfStringInt();
    private static final VIntWritable TF = new VIntWritable();
    private static final HashMap<String, Integer> postings = new HashMap<>();

    @Override
    public void map(Text path, Text content, Context context) throws IOException, InterruptedException {

        for(String term : Tokenizer.tokenize(content.toString())){
            if(postings.containsKey(term)){
                postings.put(term, postings.get(term) + 1);
            }else{
                postings.put(term, 1);
            }
        }

        TERM_DOCID.setRightElement(FileIdExtractor.extractId(path.toString()));

        for(String term : postings.keySet()){
            TERM_DOCID.setLeftElement(term);
            TF.set(postings.get(term));

            context.write(TERM_DOCID, TF);
        }

        postings.clear();
    }
}