package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by cassiano on 06/06/17.
 */
public class IndexPartitioner extends Partitioner <PairOfStrings, VIntWritable>{
    @Override
    public int getPartition(PairOfStrings k, VIntWritable v, int numPartitions) {
        return (k.getLeftElement().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
