#!/usr/bin/env python
# coding=utf-8
import os
import sys
import re
from pyspark import SparkConf, SparkContext

input_fasta_file ='/home/yueyao/Spark/00.data/both.fa'

conf = SparkConf().setMaster("local").setAppName("Yue Yao app")
sc = SparkContext(conf = conf)
fasta_file = sc.textFile(input_fasta_file)

#这里是对fasta文件进行转化操作，过滤掉reads的名称
reads_fa = fasta_file.filter(lambda line :">" not in line)

#这个函数用来将reads打断成kmer，这里的kmer是25,返回一个列表
def map_file(line):
    seq_lis=[]
    for i in range(len(line)-25+1):
        sub_seq = line[i:i+25]
        seq_lis.append(sub_seq)
    return seq_lis

kmer_list = reads_fa.flatMap(map_file)
#对打断的kmer进行计数
kmer_count = kmer_list.map(lambda id:(id,1))
kmer_total_count = kmer_count.reduceByKey(lambda a,b:(a+b))
#这里过滤掉了含有N的kmer
kmer_not_contain_N = kmer_total_count.filter(lambda line :"N" not in line)
kmer_key=kmer_not_contain_N.keys()
#统计kmer的种类，并计数
kmer_vari_count = kmer_not_contain_N.map(lambda kmer_vari:(kmer_vari[1],1))
kmer_histo = kmer_vari_count.reduceByKey(lambda a,b:(a+b))
#输出kmer频数的结果
kmer_histo.saveAsTextFile('Kmer25.histo')
kmer_not_contain_N.saveAsTextFile('kmer25')
kmer_key.saveAsTextFile('kmer25_key')