import mincemeat
import csv
from nltk.tokenize import RegexpTokenizer
import collections
import os


def get_data(path):
    data = {}
    for file in os.listdir(path):
        filepath = os.path.join(path, file)
        with open(filepath, "r") as f:
            text = f.read()
            text = text.lower()
            text = text.replace("'", "")
            tokenizer = RegexpTokenizer(r'\w+')
            text = tokenizer.tokenize(text)
            data.update({file: text})
    return data


def get_results(path, results):
    for file in os.listdir(path):
        for elem in results.values():
            if file not in elem.keys():
                elem.update({file: 0})
    return results


def write_to_csv(results):
    od = collections.OrderedDict(sorted(results.items()))

    firstRow = [""]
    for file in os.listdir(path):
        firstRow.append(file)

    with open('words.csv', 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(firstRow)
        for key, value in od.items():
            row = [key]
            value = collections.OrderedDict(sorted(value.items()))
            for k, v in value.items():
                row.append(v)
            writer.writerow(row)


def mapfn(k, v):
    for w in v:
        d = {k: 1}
        yield w, d


def reducefn(k, vs):
    tmp = {}
    for mas in vs:
        for i in mas.keys():
            if i not in tmp:
                tmp.update({i: mas[i]})
            else:
                tmp[i] += mas[i]
    return tmp


path = "/home/kristofer76/6 cource/nikolsky/task5/sherlock/"
dict_data = get_data(path)

s = mincemeat.Server()
s.datasource = dict_data
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
results = get_results(path, results)
write_to_csv(results)


