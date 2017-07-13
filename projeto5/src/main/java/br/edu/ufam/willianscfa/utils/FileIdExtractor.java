package br.edu.ufam.willianscfa.utils;

/**
 * Created by cassiano on 08/06/17.
 */
public class FileIdExtractor {
    public static int extractId(String path){
        // o cara que converte o path do documento em um int pra escrever com vInts
        return Integer.parseInt(path.substring(path.length() - 3));
    }
}
