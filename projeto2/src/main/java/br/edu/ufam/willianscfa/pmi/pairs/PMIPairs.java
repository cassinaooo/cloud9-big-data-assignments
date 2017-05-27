/**
 * Bespin: reference implementations of "big data" algorithms
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.edu.ufam.willianscfa.pmi.pairs;

import br.edu.ufam.willianscfa.utils.PairOfStrings;
import br.edu.ufam.willianscfa.wordcount.LineProbMapper;
import br.edu.ufam.willianscfa.wordcount.LineProbReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

/**
 * Simple word count demo.
 */
public class PMIPairs extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PMIPairs.class);

    /**
     * Creates an instance of this tool.
     */
    private PMIPairs() {
    }

    private static final class Args {
        @Option(name = "--input", metaVar = "[path]", required = false, usage = "input path")
        String input = "data/Shakespeare.txt";

        @Option(name = "--intermediate", metaVar = "[path]", required = false, usage = "output path")
        String intermediatePath = "wc";

        @Option(name = "--output", metaVar = "[path]", required = false, usage = "output path")
        String output = "pmi-pairs";

        @Option(name = "--reducers", metaVar = "[num]", usage = "number of reducers")
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

        LOG.info("Tool: " + PMIPairs.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - intermediate wc path: " + args.intermediatePath);
        LOG.info(" - number of reducers: " + args.numReducers);

        Configuration conf = getConf();
        Job wordcount_job = Job.getInstance(conf);
        wordcount_job.setJobName("WordCount");
        wordcount_job.setJarByClass(PMIPairs.class);

        wordcount_job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(wordcount_job, new Path(args.input));
        FileOutputFormat.setOutputPath(wordcount_job, new Path(args.intermediatePath));

        wordcount_job.setMapOutputKeyClass(Text.class);
        wordcount_job.setMapOutputValueClass(IntWritable.class);
        wordcount_job.setOutputKeyClass(Text.class);
        wordcount_job.setOutputValueClass(DoubleWritable.class);
        wordcount_job.setOutputFormatClass(TextOutputFormat.class);

        wordcount_job.setMapperClass(LineProbMapper.class);
        wordcount_job.setReducerClass(LineProbReducer.class);

        // Delete the output directory if it exists already.
        Path intermediateFilePath = new Path(args.intermediatePath);
        FileSystem.get(conf).delete(intermediateFilePath, true);

        long startTime = System.currentTimeMillis();
        wordcount_job.waitForCompletion(true);
        LOG.info("WordCount finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


        Job pmi_job = Job.getInstance(conf);
        pmi_job.setJobName("WordCount");
        pmi_job.setJarByClass(PMIPairs.class);

        pmi_job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(pmi_job, args.input);
        FileOutputFormat.setOutputPath(pmi_job, new Path(args.output));

        pmi_job.setMapOutputKeyClass(PairOfStrings.class);
        pmi_job.setMapOutputValueClass(IntWritable.class);
        pmi_job.setOutputKeyClass(PairOfStrings.class);
        pmi_job.setOutputValueClass(DoubleWritable.class);
        pmi_job.setOutputFormatClass(TextOutputFormat.class);

        pmi_job.setMapperClass(PairsMapper.class);
        pmi_job.setReducerClass(PairsReducer.class);

        // Delete the output directory if it exists already.
        Path output = new Path(args.output);
        FileSystem.get(conf).delete(output, true);

        startTime = System.currentTimeMillis();
        pmi_job.waitForCompletion(true);
        LOG.info("PMI finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PMIPairs(), args);
    }
}
