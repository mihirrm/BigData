from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans,KMeansModel
from pyspark import SparkContext
from collections import defaultdict
from collections import defaultdict

sc = SparkContext("homework3","part1")
def read_data(itemuser_path,movies_path):
	user_mat = sc.textFile(itemuser_path)
	user_mat.take(2)
	data = user_mat.map(lambda x : array([float(y) for y in x.split(' ')]))
	t_data = data.map(lambda x : x[1:])
	t_data.take(2)
	movies_load = sc.textFile(movies_path)
	movies_load_split = movies_load.map(lambda x : x.split("::"))
	movies_name = movies_load_split.map(lambda x : (float(x[0]),x[1]))
	movies_name.take())
	return data,t_data,movies_name
	
	
def k_means_train(data,t_data,movies_name):
	clustering = KMeans.train(t_data,10,maxIterations=100,initializationMode = 'random')
	output = data.map(lambda x : (x[0],clustering.predict(x[1:])))
	join_result = movies_name.join(output)
	join_result.take(2)
	join_r = join_result.map(lambda x : (x[1][1],x[1][0]))
	join_r.take(2)
	clusters = join_r.groupByKey().collect()
	result_dict = defaultdict(list)
	for i in clusters:
		counter = 0
		result_dict['cluster'] = i[0]
		for j in  (i[1]):
			result_dict[i[0]].append(j)
			counter = counter + 1
			if counter ==5 :
				break
	return result_dict 


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--itemusermat_file_path')
	parser.add_argument('--movies_file_path')
	args = parser.parse_args()
	data,itemuser,movies = read_data(args.itemusermat_file_path,args.movies_file_path)
	print (k_means_train(data,itemuser,movies))
	

if __name__ == '__main__':
	main()
