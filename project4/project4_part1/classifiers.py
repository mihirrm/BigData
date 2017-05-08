from pyspark import SparkContext
import argparse
#sc = SparkContext("local","app")
from numpy import array
from math import sqrt
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree,DecisionTreeModel

sc = SparkContext("homework3","part2")
def parse_line(line):
	parts = line.split(',')
	label = float(parts[-1])
	features = Vectors.dense([float(x) for x in parts[0:-1]])
	return LabeledPoint(label,features)

def read_file(file_path):
	da = sc.textFile(file_path).map(parse_line)
	return da

def test_accuracy_naive_bayes(file_path):
	da = read_file(file_path)
	train_data,test_data = da.randomSplit([0.6,0.4])
	naive_bayes_model = NaiveBayes.train(train_data,1.0)
	prediction = test_data.map(lambda x : 	(naive_bayes_model.predict(x.features),x.label))
	return accuracy_test(prediction)

def test_accuracy_decision_tree(file_path):
	da = read_file(file_path)
	train_data,test_data = da.randomSplit([0.6,0.4])
	decision_tree_model = DecisionTree.trainClassifier(train_data,numClasses = 8,categoricalFeaturesInfo = {},impurity='gini',maxDepth=5,maxBins=32)
	predictions = decision_tree_model.predict(test_data.map(lambda x : x.features))
	label = test_data.map(lambda x : x.label).zip(predictions)
	return accuracy_test(label)

def accuracy_test(label):
	accuracy = 1.0 * label.filter(lambda (k,v): k==v).count()/label.count()
	return accuracy

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--input_path')
	args = parser.parse_args()
	print ('Naive Bayes Test Accuracy     ',test_accuracy_naive_bayes(args.input_path)*100)
	print ('Decision Tree Accuracy     ',test_accuracy_decision_tree(args.input_path)*100)

if __name__ == '__main__':
	main()