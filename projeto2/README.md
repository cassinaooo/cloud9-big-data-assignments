### Setup
[Container Docker Cloudera Quickstart](https://hub.docker.com/r/cloudera/quickstart/) rodando em uma máquina Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz de 8 núcleos e 16 GB de RAM. HDD. Modo de cluster pseudo-distribuído.

### Estratégia utilizada

Para ambas as versões (pairs e stripes), foram executados 2 jobs mapreduce executando com 5 reducers em todos os passos.
O primeiro job a executar é uma versão modificada do WordCount.

Para cada par este job gera a tupla ```(x, P(X))``` onde x é um termo único no documento e P(x) é a probabilidade deste termo ocorrer em uma linha aleatória do documento. 

Esta lista de tuplas é armazenada em um dicionário (Hash) na memória de cada reducer em seu método ```setup()``` e utilizada no calculo do PMI.

Este job leva em média 23 segundos para executar.

### Pairs

- Utilizando a implementação de PairOfStrings, presente no pacote [lintools](https://github.com/lintool/tools).
- Tempo médio de execução: 33 segundos.
- Modelo de emissão de pares como nos slides da disciplina.

### Stripes

- Utilizando como stripes a implementação de HMapStFW, presente no pacote [lintools](https://github.com/lintool/tools).
- Tempo médio de execução: 24 segundos.
- Modelo de emissão de stripes como nos slides da disciplina.

### Shakespeare

- Pares de PMI: 38599
- Par com o pmi mais alto: (anjou, maine): 3.633. Condados vizinhos na França na época de Shakespeare. São frequentemente referidos juntos pois sua posse é negociada em um extenso trecho na peça Henrique VI, Parte 2 (The Second Part of King Henry the Sixth).
- 3 pares com o PMI mais alto para "life": [save: 1.274, man's: 0.979, death: 0.738] 
- 3 pares com o PMI mais alto para "love": [dearly: 1.181, hate: 1.084, hermia: 0.895]. Hermia é uma personagem envolvida num triângulo amoroso com Lysander e Demetrius em A Midsummer Night's Dream (Sonho de uma Noite de Verão).

### Wikimedia

Não consegui rodar pois meu computador congelou na fase de reducer para o modo pairs e para o modo stripes em todas as vezes que tentei. :(

Tentei utilizar o arquivo simplewiki-20170520-pages-articles-multistream.xml.bz2 disponível [aqui](https://dumps.wikimedia.org/simplewiki/20170520/), pois o link do blog estava fora do ar. Como está em XML, acredito que há muito clutter alí que possivelmente prejudica o método, pelo que vi das primeiras 200 linhas do arquivo. Garbage IN, garbage OUT.

### Misc 
O script run.sh que acompanha este projeto realiza o download da coleção Shakespeare, a insere no hdfs, roda os jobs e dá como saída os 10 pares e 10 stripes com maior ocorrência. A pasta python contém os scripts utilizados na ordenação dos resultados (favor não abrir).

É possível customizar o comportamento do programa utilizando-se as opções de linha de comando (tanto para o stripes quanto para o pairs):

--output: "Determina a pasta de saída dos reducers finais no HDFS" default=(pmi-stripes | pmi-pairs)

--input: "Determina o arquivo para qual calcular o PMI" default=data/Shakespeare.txt

--intermediate: "Determina a pasta na qual será salvo os arquivos do primeiro Job" default=wc
