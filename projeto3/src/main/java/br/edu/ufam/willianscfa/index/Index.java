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

package br.edu.ufam.willianscfa.index;

import br.edu.ufam.willianscfa.utils.PairOfStringInt;
import br.edu.ufam.willianscfa.utils.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class Index extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Index.class);

    /**
     * Creates an instance of this tool.
     */
    private Index() {
    }

    private static final class Args {
        @Option(name = "--input", metaVar = "[path]", required = false, usage = "input path")
        String input = "splitted-shakespeare";

        @Option(name = "--output", metaVar = "[path]", required = false, usage = "output path")
        String output = "index";

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

        LOG.info("Tool: " + Index.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);

        Configuration conf = getConf();
        Job index_job = Job.getInstance(conf);


        index_job.setJobName("Index");
        index_job.setJarByClass(Index.class);
        index_job.setNumReduceTasks(args.numReducers);

        index_job.setInputFormatClass(WholeFileInputFormat.class);

        FileInputFormat.setInputPaths(index_job, args.input);
        FileOutputFormat.setOutputPath(index_job, new Path(args.output));

        index_job.setMapOutputKeyClass(Text.class);
        index_job.setMapOutputValueClass(PairOfStringInt.class);
        index_job.setOutputKeyClass(Text.class);
        index_job.setOutputValueClass(Text.class);

        index_job.setOutputFormatClass(TextOutputFormat.class);

        index_job.setMapperClass(IndexMapper.class);
        index_job.setReducerClass(IndexReducer.class);
        index_job.setPartitionerClass(IndexPartitioner.class);

        // Delete the output directory if it exists already.
        Path output = new Path(args.output);
        FileSystem.get(conf).delete(output, true);

        long startTime = System.currentTimeMillis();
        index_job.waitForCompletion(true);
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
        ToolRunner.run(new Index(), args);
    }
}
