mkdir data
curl http://lintool.github.io/bespin-data/Shakespeare.txt > data/Shakespeare.txt
mkdir data/splitted-shakespeare
split data/Shakespeare.txt -d -a 3 data/splitted-shakespeare/shakespeare-part-
hadoop fs -put data/splitted-shakespeare
mvn clean package
hadoop jar target/projeto3-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.index.Index
hadoop jar target/projeto3-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.willianscfa.index.BooleanRetrieval -index index
hadoop fs -rm -r index splitted-shakespeare
echo 'Arquivo 014 contem outrageous AND fortune: '
echo 'cat -n data/splitted-shakespeare/shakespeare-part-014 | grep 'outrageous\|fortune''
cat -n data/splitted-shakespeare/shakespeare-part-014 | grep 'outrageous\|fortune'
rm -r data
