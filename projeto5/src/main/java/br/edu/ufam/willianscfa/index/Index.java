/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.Tokenizer;
import br.edu.ufam.willianscfa.utils.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Index extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Index.class);

    public static final byte[] CF = "p".getBytes();

    private static final class MyMapper extends Mapper<IntWritable, Text, Text, PairOfInts> {
        private static final Text WORD = new Text();
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<>();

        @Override
        public void map(IntWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(doc.toString());

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                WORD.set(e.getLeftElement());
                context.write(WORD, new PairOfInts(docno.get(), e.getRightElement()));
            }
        }
    }

    private static final class MyReducer extends
            TableReducer<Text, PairOfInts, Text> {

        @Override
        public void reduce(Text key, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {
            Iterator<PairOfInts> iter = values.iterator();
            ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

            int df = 0;
            while (iter.hasNext()) {
                postings.add(iter.next().clone());
                df++;
            }

            // Sort the postings by docno ascending.
            Collections.sort(postings);

            Put put;

            for(PairOfInts pair : postings){
                // key => term
                put = new Put(Bytes.toBytes(key.toString()));
                // column family, column qualifier, value
                // "p", doc_no, frequency
                put.addColumn(CF, Bytes.toBytes(pair.getLeftElement()), Bytes.toBytes(pair.getRightElement()));
                context.write(null, put);
            }
        }
    }

    private Index() {}

    private static final class Args {
        @Option(name = "--input", metaVar = "[path]", usage = "input path")
        String input = "splitted-shakespeare";

        @Option(name = "--outputTable", metaVar = "[path]", usage = "outputTable name")
        String outputTable = "indexTable";

        @Option(name = "--reducers", metaVar = "[path]", usage = "reducers num")
        int numReducers = 5;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        // Hbase setup

        Configuration conf = getConf();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        HBaseAdmin admin = new HBaseAdmin(hbaseConfig);

        if (admin.tableExists(args.outputTable)) {
            LOG.info(String.format("Table '%s' exists: dropping table and recreating.", args.outputTable));
            LOG.info(String.format("Disabling table '%s'", args.outputTable));
            admin.disableTable(args.outputTable);
            LOG.info(String.format("Droppping table '%s'", args.outputTable));
            admin.deleteTable(args.outputTable);
        }

        // cria a tabela

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(args.outputTable));
        HColumnDescriptor hColumnDesc = new HColumnDescriptor(CF);
        tableDesc.addFamily(hColumnDesc);

        admin.createTable(tableDesc);
        LOG.info(String.format("Successfully created table '%s'", args.outputTable));

        admin.close();

        LOG.info("Tool: " + Index.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - outputTable: " + args.outputTable);

        Job job = Job.getInstance(getConf());
        job.setJobName(Index.class.getSimpleName());
        job.setJarByClass(Index.class);

        job.setNumReduceTasks(args.numReducers);

        job.setInputFormatClass(WholeFileInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args.input));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        TableMapReduceUtil.initTableReducerJob(args.outputTable, MyReducer.class, job);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Index(), args);
    }
}
