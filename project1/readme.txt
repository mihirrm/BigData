## Description of the file adn its content
the jar folder contains following :-
1) part_1.jar for part1 of homework
2) part_2.jar for part2 of homework
3) part_3.jar for part3 of homework
4) part_4.jar for part4 of homework

the final_output contains output folder, but actual output are in folder as follows:-
1) part_1 contains output for part1 of homework
2) part_1 contains output for part2 of homework
3) part_1 contains output for part3 of homework
4) part_1 contains output for part4 of homework

the code folder contains code file for assignment:-
1) HomewrokPart1.java contains source code for part1 of homework
2) HomewrokPart2.java contains source code for part2 of homework
3) HomewrokPart3.java contains source code for part3 of homework
4) HomewrokPart4.java contains source code for part4 of homework

Steps to run the homework1:

1)	please set up the input and output and jar files in hdfs by performing following steps

1.1) 	put the jar file in username@csgrads1.utdallas.edu repository this can be done through Winscp in windows.

1.2)	this can be acheived by following command 
		hdfs dfs -mkdir /username(netid)

1.3) 	then put the jar file from unix to hadoop
		this can be acheived by following command
		hdfs dfs -put /path of unix/   /path in hdfs/

1.4)	put business.csv inside hdfs folder by following command:
		hdfs dfs -put /path of unix/ /path in hfds/
	
1.5)	put review.csv inside hdfs folder by following command:
		hdfs dfs -put /path of unix/ /path in hfds/
	
1.6)	put user.csv inside hdfs folder by following command:
		hdfs dfs -put /path of unix/ /path in hfds/
	
1.7)	create an output directory in hdfs 
		hdfs dfs -mkdir /path in hdfs/output	


		
2) to run the homework follow the following steps:- 


2.1) homework part 1 takes (input/business)  (output) 
2.1.1) 	by following command run part 1 
	hadoop jar jarfilename(jar) packagename.classname(class)     /input filepath/    /output filepath/

2.1.2)  to view first part output:
        hdfs dfs -cat /output filepath/part-00000
		
		

		
2.2) 	homework part 2 takes (input/review)       (intermediateoutput1)    (output)
2.2.1) 	by following command run part 1 
		hadoop jar jarfilename(jar) packagename.classname(class)       /input filepath/     /intermediate_output filepath/     /output filepath/

2.2.2)  to view second part output:
        hdfs dfs -cat /output filepath/part-00000

		
		
2.3) 	homework part 3 args structure (input/review) (intermediateoutput1) (intermediateoutput2) (input/business file) (output) 
2.3.1) 	by following command run part 1 
		hadoop jar jarfilename(jar) packagename.classname(class) /input fielpath(review)/       /intermediate_output_1 filepath/
		/intermediate_output_2 filepath/     		/input filepath(business )/
		/output filepath/

2.3.2)  to view third part output:
        hdfs dfs -cat /output filepath/part-00000

		
2.4)    homework part 4 args strucuture   (input/business)   (input/review)   (output)   
2.4.1) 	by following command run part 1 
		hadoop jar jarfilename(jar) packagename.classname(class) /input filepath of business csv/business.csv(note please include business.csv in filepath)         /input filepath of review/
		/output filepath/

2.4.2)  to view fourth part output:
        hdfs dfs -cat /output filepath/part-r-00000
		
				
note :-  to get output folder from hdfs to unix write following command
hdfs dfs -get /hdfs path of output file/
with this we can see the output folder in Winscp adn we can download it from there.