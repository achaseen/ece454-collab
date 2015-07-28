register Part3.jar;
-- Load our data --
samples_one = LOAD '$input' USING PigStorage(',');
samples_two = LOAD '$input' USING PigStorage(',');
--Cross two data samples
crossSamples = CROSS samples_one, samples_two;
--Call UDF to calculate similarity
similarities = FOREACH crossSamples GENERATE FLATTEN(Part3($0..)) AS (s1:chararray, index1:int, s2:chararray, index2:int, similarity:double);
--Filter out any pairs with the index of the second sample lower or equal to the index of the first sample
distinct_similarities = FILTER similarities BY ($3 > $1) AND ($4 > 0);
--Select sample names and the similarity value
selected_similarities = FOREACH distinct_similarities GENERATE $0,$2,$4;
ordered_similarities = ORDER selected_similarities BY $0;
STORE selected_similarities INTO '$output' USING PigStorage(',');
