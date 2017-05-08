### part 1 
import argparse
from pyspark import SparkContext, SparkConf
def run_program(business_file,review_file,output_file):
        sc = SparkContext("local","app")
	business  = sc.textFile(business_file)
	business_data = business.map(lambda line : line.split("::"))
	stanford_business = business_data.filter(lambda x :'Stanford' in x[1])
	stanford_business_dict  = stanford_business.map(lambda x: (x[0],x))
	review  = sc.textFile(review_file)
	review_data = review.map(lambda x: x.strip().split("::"))
	review_data_dict  = review_data.map(lambda x: (x[2],x))
	res = review_data_dict.join(stanford_business_dict)

	l_out = []
	for x in  res.collect():
		st = x[1][0][1]+'        '+x[1][0][3]
		l_out.append(st)
	
	print (l_out)
	set_out = list(set(l_out))
	output_data = sc.parallelize(set_out)	
	output_data.saveAsTextFile(output_file)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--business_file')
    parser.add_argument('--review_file')
    parser.add_argument('--output_file')
    args = parser.parse_args()
    run_program(args.business_file,args.review_file,args.output_file)

if __name__ == '__main__':
	main()
