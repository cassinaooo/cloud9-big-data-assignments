package br.edu.ufam.willianscfa.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cassiano on 26/05/17.
 */
public class DirReader {

    // um monte de baboseira pra ler do HDFS, retirei do setup do
    // reducer e coloquei aqui pra dar mais legibilidade

    public static Map<String, Double> getWordCount(Reducer.Context context){
        Map<String, Double> termTotals = new HashMap<>();

        Configuration conf = context.getConfiguration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Path inDir = new Path("wc");
        Path inFile;

        try {
            assert fs != null;
            for (FileStatus fileStatus : fs.listStatus(inDir)){
                inFile = fileStatus.getPath();

                if(!fs.exists(inFile)){
                    throw new IOException("Arquivo não encontrado: " + inFile.toString());
                }

                BufferedReader reader;

                try{
                    FSDataInputStream in = fs.open(inFile);
                    InputStreamReader inStream = new InputStreamReader(in);
                    reader = new BufferedReader(inStream);

                } catch(FileNotFoundException e){
                    throw new IOException("Arquivo não encontrado " + inFile.toString());
                }


                String line = reader.readLine();
                while(line != null){

                    String[] parts = line.split("\\s+");

                    termTotals.put(parts[0], Double.parseDouble(parts[1]));

                    line = reader.readLine();
                }

                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return termTotals;
    }
}
