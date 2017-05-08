business_file_load = LOAD '$file_1' AS Line;
filter_business = FOREACH business_file_load GENERATE FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(Line,'(.*)\\:\\:(.*)\\:\\:(.*)')) AS (business_id,full_address,categories);

review_file_load = LOAD '$file_2' AS Line;
filter_review = FOREACH review_file_load GENERATE FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(Line,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) AS (review_id,user_id,business_id,stars);

business_at_stanford = FILTER filter_business BY (full_address matches '.*Stanford.*');
 
join_result = JOIN business_at_stanford by business_id, filter_review by business_id;

distinct_stanford_result = DISTINCT join_result;

column_result = foreach distinct_stanford_result generate $4,$6;

store column_result into '$output';

answer = LIMIT column_result 10;

dump answer;

