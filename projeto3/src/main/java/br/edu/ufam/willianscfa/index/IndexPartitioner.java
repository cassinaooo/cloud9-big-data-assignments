package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.PairOfStringInt;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by cassiano on 06/06/17.
 */
public class IndexPartitioner extends Partitioner <Text, PairOfStringInt>{
    @Override
    public int getPartition(Text text, PairOfStringInt pairOfStringInt, int numPartitions) {
        return 0;
    }
}
