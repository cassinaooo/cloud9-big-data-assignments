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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class BooleanRetrieval extends Configured implements Tool {
    private Stack<Set<Integer>> stack;
    private static final String TABLE = "indexTable";
    private static final String CF = "p";

    private static Configuration hbaseConfig;
    private static HConnection hbaseConnection;
    private static HTableInterface table;


    private BooleanRetrieval() {
    }

    private void initialize(Configuration conf) throws IOException {
        // suprimir as saidas INFO do Zookeeper que sao bem chatas
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
        Logger.getLogger("org.apache.hadoop.hbase.zookeeper").setLevel(Level.WARN);
        Logger.getLogger("org.apache.hadoop.hbase.client").setLevel(Level.WARN);

        hbaseConfig = HBaseConfiguration.create(conf);
        hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
        table = hbaseConnection.getTable(TABLE);
        stack = new Stack<>();
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
        System.out.print("Docs: ");
        System.out.println(set);
    }

    private void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    private void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

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

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    private Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<>();

        for (PairOfInts pair : fetchPostings(term)) {
            set.add(pair.getLeftElement());
        }

        return set;
    }

    private ArrayList<PairOfInts> fetchPostings(String term) throws IOException {

        Get get = new Get(Bytes.toBytes(term));
        Result result = table.get(get);

        NavigableMap<byte[], byte[]> row = result.getFamilyMap(CF.getBytes());
        ArrayList<PairOfInts> postings = new ArrayList<>();

        for (byte[] docno : row.keySet()) {
            postings.add(new PairOfInts(Bytes.toInt(docno), Bytes.toInt(row.get(docno))));
        }

        return postings;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        long startTime = System.currentTimeMillis();
        initialize(getConf());

        String[] queries = {"outrageous fortune AND", "means deceit AND", "white red OR rose AND pluck AND", "unhappy outrageous OR good your AND OR fortune AND" };

        for (String q : queries) {
            System.out.println("Query: '" + q + "': ");
            runQuery(q);
        }

        System.out.println("Queries completed in " + (System.currentTimeMillis() - startTime) + " ms");

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrieval(), args);
    }
}
