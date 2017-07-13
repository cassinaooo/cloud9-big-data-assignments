## Execução

É possível executar os testes utilizando o script run.sh presente na raiz deste trabalho. Ele realiza as seguintes tarefas:
- Baixa a base sample-large de http://lintool.github.io/Cloud9/docs/exercises/sample-large.txt
- Carrega os dados no HDFS
- Realiza o parse da base para construir os registros Writables
- Faz o particionamento da coleção
- Executa 20 iterações do PageRank personalizado para 3 nós
- Exibe os 10 maiores valores para cada um dos nós de origem
