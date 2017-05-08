import numpy
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext
import argparse

sc = SparkContext("homework3","part3")
def return_clean_data(filepath):
	rating = sc.textFile(filepath)
	rat_strip = rating.map(lambda x : x.split('::'))
	act_r = rat_strip.map(lambda x : Rating(int(x[0]),int(x[1]),float(x[2])))
	return act_r

def ALS_model(act_r):	
	train_data,test_data = act_r.randomSplit([0.6,0.4])
	rank = 10
	iterations = 10
	ALS_model = ALS.train(train_data,rank,iterations)
	predict_test_data = test_data.map(lambda x:(x[0],x[1]))
	predictions = ALS_model.predictAll(predict_test_data).map(lambda x : ((x[0],x[1]),x[2]))
	join_result = test_data.map(lambda x :((x[0],x[1]),x[2])).join(predictions)
	mse = join_result.map(lambda r: (r[1][0] - r[1][1])**2).mean()
	return mse


def main():
	parser = argparse.ArgumentParser()
	parser.add_argumet('--input_path')
	args = parser.parse_args()
	print ('ALS Model Accuracy     ',(1-ALS_model(return_clean_data(args.input_path)))*100)
	

if __name__=='__main__':
	main()