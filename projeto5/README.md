## Preparação dos dados

A coleção Shakespeare foi dividida em arquivos de 1000 linhas, resultando em 122 arquivos. O último arquivo contém 458 linhas. Cada arquivo foi encaminhado a entrada de uma instância de Mapper, utilizando a classe WholeFileInputFormat e WholeFileRecordReader. Estas são responsáveis por impedir a divisão dos arquivos e convertê-los em uma entrada para o Mapper da forma <IntWritable, Text> onde a chave é o id do arquivo e o valor é o conteúdo de todo o arquivo. 

## HBase
Este trabalho é uma adaptação do código presente no repositório [lintool](https://github.com/lintool/Cloud9/tree/master/src/main/java/edu/umd/cloud9/example/ir) para fazer o uso do HBase, ao invés de escrever as saídas dos reducers diretamente no HDFS.
É criada uma tabela chamada ```indexTable``` com a seguinte estrutura
```
t: {
    p: {
        t0: p:id_1: f,
        t0: p:id_2: f        
    }
}
```

aplicando a notação de mapa multidimensional utilizado na [documentação do HBase](https://hbase.apache.org/book.html#conceptual.view). Neste caso t é o termo (chave), p a Column Family e f é a frequência com o que o termo t aparece no documento de id id_i. Existe apenas uma timestamp (t0) para todas as frequências, pois esses valores são populados todos de uma só vez.

Essa estrutura é uma representação do índice invertido com postings de cada termo, utilizando o modelo de dados BigTable. A classe ```Index.java``` realiza a criação da tabela e faz a inserção dos dados em seu Reducer. E a classe ```BooleanRetrieval.java``` consulta a tabela HBase para recuperar os postings de acordo com as *queries* do usuário.

## Execução

É possível executar os testes utilizando o script run.sh presente na raiz deste trabalho. Ele realiza as seguintes tarefas:
- Baixa a base Shakespeare.txt
- Divide a base em arquivos de 1000 linhas 
- Adiciona a coleção no HDFS
- Constrói o índice, e roda as consultas.
- Remove os arquivos temporário utilizados.