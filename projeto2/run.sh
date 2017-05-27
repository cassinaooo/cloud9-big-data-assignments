mkdir data
curl http://lintool.github.io/bespin-data/Shakespeare.txt > data/Shakespeare.txt
hadoop fs -put data
mvn clean package
hadoop jar target/projeto2-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.pmi.stripes.PMIStripes
hadoop fs -copyToLocal pmi-stripes
cat pmi-stripes/part-r-0000* > pmi-stripes/concat-out
python python/sorter-stripes.py pmi-stripes/concat-out > pmi-stripes/sorted-out 
hadoop jar target/projeto2-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.pmi.pairs.PMIPairs
hadoop fs -copyToLocal pmi-pairs
cat pmi-pairs/part-r-0000* > pmi-pairs/concat-out
python python/sorter-pairs.py pmi-pairs/concat-out > pmi-pairs/sorted-out
echo "-------- PRIMEIROS 10 RESULTADOS PARA PAIRS ------------"
head pmi-pairs/sorted-out
echo "-------- PRIMEIROS 10 RESULTADOS PARA STRIPES ------------"
head pmi-stripes/sorted-out