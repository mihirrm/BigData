import operator
import argparse
from pyspark import SparkContext, SparkConf

def program_run(input_file,output_file):
	sc = SparkContext("local","app")
	inps = sc.textFile(input_file)
	split_input = inps.map(lambda line : line.split("\t"))
	format_input = split_input.map(lambda line : (line[1],int(line[2])))
	output_value = format_input.combineByKey(int,lambda x,y :(x+int(y)),lambda x,y : (x+int(y)))
	output_value = output_value.sortBy(lambda x:x[0])
	for i in output_value.collect():
		print (i)

	output_value.saveAsTextFile(output_file)


def main():
	parser = argparse.ArgumentParser()	
	parser.add_argument('--input')
	parser.add_argument('--output')
	args= parser.parse_args()
	program_run(args.input,args.output)

	
if __name__ == '__main__':
	main()
	
	

