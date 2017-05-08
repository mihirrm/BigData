business_file_load = LOAD '$file_1' AS Line;  
filter_business = FOREACH business_file_load GENERATE FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(Line,'(.*)\\:\\:(.*)\\:\\:(.*)')) AS (business_id,full_address,categories);

review_file_load = LOAD '$file_2' AS Line;   
filter_review = FOREACH review_file_load GENERATE FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(Line,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) AS (review_id,user_id,business_id,stars);

grouped_review_by_business_id = group filter_review by business_id;

average_rating = foreach grouped_review_by_business_id GENERATE group as business_id, AVG(filter_review.stars);

business_not_at_palo_alto = FILTER filter_business BY not(full_address matches '.*Palo Alto, CA.*') AND (full_address matches '.*CA.*');

join_result = join average_rating by business_id, business_not_at_palo_alto by business_id;

distinct_result = DISTINCT join_result;

descending_result = order distinct_result by $1 desc;

column_result = foreach descending_result generate $2,$3,$4;

store column_result into '$output';

answer = LIMIT column_result 10;

dump answer;
