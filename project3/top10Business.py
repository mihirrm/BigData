import operator
import argparse
from pyspark import SparkContext, SparkConf

def run_program(business_file,review_file,output_file):
        sc = SparkContext("local","app")
	business  = sc.textFile(business_file)
	business_data = business.map(lambda line : line.split("::"))
	business_data_dict = business_data.map(lambda x : (x[0],x))
	review  = sc.textFile(review_file)
	review_data = review.map(lambda x: x.strip().split("::"))
	review_data_dict  = review_data.map(lambda x: (x[2],x[3]))
	review_avg = review_data_dict.map(lambda x:(x[0],float(x[1])))
	count = sc.broadcast(review_avg.countByKey())
	adder = review_avg.reduceByKey(operator.add)
	average_value = adder.map(lambda x:(x[0],x[1]/count.value[x[0]]))
	reverse_sort = average_value.takeOrdered(10,key=lambda x: -x[1])
	top_10_rating = sc.parallelize(reverse_sort)	
	join_result = top_10_rating.join(business_data_dict)
	distinct_set = []
	for x in join_result.collect():
		st = x[0]+'  '+x[1][1][1]+'  '+x[1][1][2]+'  '+str(x[1][0])
		distinct_set.append(st)

	distinct_list = list(set(distinct_set))	
	print (distinct_list)
	output_data = sc.parallelize(distinct_list)	
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


	
	

