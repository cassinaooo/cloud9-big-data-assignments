lines_list = []

with open("../pmi-pairs/concat-out") as f:
    for line in f:
        lines_list.append((line.strip(), line.strip().split("\t")[1]))

lines_list = sorted(lines_list, key=lambda term: -float(term[1]))

for pair in lines_list:
    print pair[0]