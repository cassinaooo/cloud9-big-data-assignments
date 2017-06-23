mkdir -p data
curl http://lintool.github.io/Cloud9/docs/exercises/sample-large.txt > data/sample-large.txt
hadoop fs -put data/sample-large.txt
mvn clean package
hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.pagerank.BuildPageRankRecords -input sample-large.txt -output willianscfa_PageRankRecords -numNodes 1458 -sources 9627181,9370233,10207721
hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.pagerank.PartitionGraph -input willianscfa_PageRankRecords -output willianscfa_PageRank/iter0000 -numPartitions 5 -numNodes 1458
hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.pagerank.RunPageRankBasic -base willianscfa_PageRank -numNodes 1458 -start 0 -end 20 -sources 9627181,9370233,10207721
hadoop jar target/projeto4-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.pagerank.FindMaxPageRankNodes -input willianscfa_PageRank/iter0020 -top 10 -sources 9627181,9370233,10207721
