## Preparação dos dados

A coleção Shakespeare foi dividida em arquivos de 1000 linhas, resultando em 122 arquivos. O último arquivo contém 458 linhas. Cada arquivo foi encaminhado a entrada de uma instância de Mapper, utilizando a classe WholeFileInputFormat e WholeFileRecordReader. Estas são responsáveis por impedir a divisão dos arquivos e convertê-los em uma entrada para o Mapper da forma <Text, Text> onde a chave é o nome do arquivo e o valor é o conteúdo de todo o arquivo. 

## Fase Map
Com o path e o texto de cada arquivo, convertemos o path em um id único e emitimos pares (t, doc_id) -> k para cada termo t no documento doc_id com frequência k. Para garantir que todos as entradas de um mesmo termo vão ao mesmo reducer, implementamos um Partitioner que considera apenas o termo para levar os dados ao Reducer correto.

## Fase Reduce
Com a garantia de ordenação na fase Shuffle and Sort, podemos calcular as d-gaps e emitir um array de postings para cada termo. Emitimos um par Text -> ArrayListWritable disponível em [lintools](https://github.com/lintool/tools). Este ArrayListWritable é composto por vários PairOfVInts, uma implementação modificada de [PairOfInts](https://github.com/lintool/tools/blob/master/lintools-datatypes/src/main/java/tl/lin/data/pair/PairOfInts.java), que escreve ints de tamanho variável (comprimidos) no disco. 
O formato de saída escolhido no Reduce foi o MapFile, uma especialização de SequenceFile, que cria um índice para facilitar a busca por chaves no arquivo principal.

## Boolean Retrieval
Partindo da implementação disponível [aqui](https://github.com/lintool/Cloud9/blob/master/src/main/java/edu/umd/cloud9/example/ir/BooleanRetrieval.java), mantivemos a parte de processamento das consultas utilizando pilha intacta. Removemos a parte que procura a linha referenciada, pois neste caso estamos interessados no arquivo. A recuperação dos arquivos foi modificada para ler os VInts armazenados. Recuperamos o índice desfazendo as d-gaps e o imprimimos na tela.

## Execução

É possível executar os testes utilizando o script run.sh presente na raiz deste trabalho. Ele realiza as seguintes tarefas:
- Baixa a base Shakespeare.txt
- Divide a base em arquivos de 1000 linhas 
- Adiciona a coleção no HDFS
- Constrói o índice, e roda as consultas.
