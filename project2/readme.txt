1) please store all the input files into HDFS directory from unix directory

2) store all required pig script into HDFS directory from unix directory and also store it in unix directory, since we are running script it will take pig script from unix path

3) steps to run pig script from command line with input and output file as parameters

3.1)command line to run script part 1 of the homework as follows:-
pig -f assignment2_part1.pig -param 'file_1 = <input path for business.csv>' -param 'file_2 = <input path for review.csv>' -param 'output = <output path to store result>'

3.2)command line to run script part 2 of the homework as follows:-
pig -f assignment2_part2.pig -param 'file_1 = <input path for business.csv>' -param 'file_2 = <input path for review.csv>' -param 'output = <output path to store result>'

3.3)command line to run script part 3 of the homework as follows:-
pig -f assignment2_part3.pig -param 'file_1 = <input path for business.csv>' -param 'file_2 = <input path for review.csv>' -param 'output = <output path to store result>'

3.4)command line to run script part 4 of the homework as follows:-
pig -f assignment2_part4.pig -param 'file_1 = <input path for business.csv>' -param 'file_2 = <input path for review.csv>' -param 'output = <output path to store result>'

