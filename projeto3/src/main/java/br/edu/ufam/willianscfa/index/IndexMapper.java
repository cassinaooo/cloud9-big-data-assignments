package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.PairOfStringInt;
import br.edu.ufam.willianscfa.utils.Tokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public final class IndexMapper extends Mapper<Text, Text, Text, PairOfStringInt> {
    private static final HashMap<String, Integer> postings = new HashMap<>();
    private static final Text TERM = new Text();
    private static final PairOfStringInt POSTING = new PairOfStringInt();

    @Override
    public void map(Text path, Text content, Context context) throws IOException, InterruptedException {

        for(String term : Tokenizer.tokenize(content.toString())){
            if(postings.containsKey(term)){
                postings.put(term, postings.get(term) + 1);
            }else{
                postings.put(term, 1);
            }
        }

        POSTING.setLeftElement(path.toString());

        for(String term : postings.keySet()){
            TERM.set(term);
            POSTING.setRightElement(postings.get(term));

            context.write(TERM, POSTING);
        }

        postings.clear();
    }
}