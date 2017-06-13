/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package br.edu.ufam.willianscfa.index;

import java.io.IOException;
import java.util.*;

import br.edu.ufam.willianscfa.utils.ArrayListWritable;
import br.edu.ufam.willianscfa.utils.PairOfVInts;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BooleanRetrieval extends Configured implements Tool {
    private MapFile.Reader index;
    private Stack<Set<Integer>> stack;

    private BooleanRetrieval() {}

    private void initialize(String indexPath, FileSystem fs) throws IOException {
        index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
        stack = new Stack<Set<Integer>>();
    }

    private void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
            }
        }

        Set<Integer> set = stack.pop();
        System.out.println("Documents -> " + set);
    }

    private void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    private void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    private void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    private Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<Integer>();

        for (PairOfVInts pair : fetchPostings(term)) {
            set.add(pair.getLeftElement());
        }

        return set;
    }

    private ArrayListWritable<PairOfVInts> fetchPostings(String term) throws IOException {
        Text key = new Text();

        ArrayListWritable<PairOfVInts> value = new ArrayListWritable<>();

        key.set(term);
        index.get(key, value);

        // refazendo o indice utilizando as dgaps

        for (int i = 1; i < value.size(); i++) {
            value.get(i).setLeftElement(value.get(i -1).getLeftElement() + value.get(i).getLeftElement());
        }

        return value;
    }


    private static final String INDEX = "index";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INDEX));

        CommandLine cmdline = null;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            System.exit(-1);
        }

        if (!cmdline.hasOption(INDEX)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(BooleanRetrieval.class.getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(-1);
        }

        String indexPath = cmdline.getOptionValue(INDEX);

        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);

        initialize(indexPath, fs);


        String[] queries = {"outrageous fortune AND", "means deceit AND", "white red OR rose AND pluck AND", "unhappy outrageous OR good your AND OR fortune AND" };

        for (String q : queries) {
            System.out.println("Query: " + q);

            runQuery(q);
            System.out.println("");
        }

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrieval(), args);
    }
}