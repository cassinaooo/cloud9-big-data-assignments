// cc WholeFileRecordReader The RecordReader used by WholeFileInputFormat for reading a whole file as a record

package br.edu.ufam.willianscfa.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//vv WholeFileRecordReader
class WholeFileRecordReader extends RecordReader<Text, Text> {

    private FileSplit fileSplit;
    private Configuration conf;
    private Text contents = new Text();
    private Text path = new Text("not-initialized");
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        // meu ->
        this.path.set(fileSplit.getPath().getName());
        // --
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, bytes, 0, bytes.length);
                this.contents.set(bytes, 0, bytes.length);
            } finally {
                IOUtils.closeStream(in);
            }


            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return path;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return contents;
    }

    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
//^^ WholeFileRecordReader