business_file_load = LOAD '$file_1' AS Line;
filter_business = FOREACH business_file_load GENERATE FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(Line,'(.*)\\:\\:(.*)\\:\\:(.*)')) AS (business_id,full_address,categories);

review_file_load = LOAD '$file_2' AS Line; 
filter_review = FOREACH review_file_load GENERATE FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(Line,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) AS (review_id,user_id,business_id,stars);

grouped_data = COGROUP filter_business by business_id, filter_review by business_id;

store grouped_data into '$output';

answer = LIMIT grouped_data 5;

dump answer;
