import sys

path = sys.argv[1]

lines_list = []

with open(path) as f:
    for line in f:
        term, stripe = (line.strip().split("\t")[0], line.strip().split("\t")[1])
        stripe = sorted(map(lambda pair: (pair.split("=")[0].strip(), float(pair.split("=")[1])), stripe[1:-1].split(",")), 
            key=lambda pair: -pair[1])
        
        lines_list.append((term, stripe))

lines_list = sorted(lines_list, key=lambda line: -abs(line[1][0][1]))


for (term, stripe) in lines_list:
    print term, "->", stripe