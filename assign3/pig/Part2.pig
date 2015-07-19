register Part2.jar
-- Load our data --
samples = LOAD '$input' USING PigStorage(',');
-- Find out how many samples there are --
total_samples = GROUP samples ALL;
sample_count = FOREACH total_samples GENERATE COUNT(samples) AS count:long;
-- Call UDF. This function gives us a bag of tuples { (gene_name, genes-related-value), (name, value), ... } --
related_genes = FOREACH samples GENERATE FLATTEN(Part2( (bag{tuple()}) TOBAG($1..) )) AS (name:chararray, value:double);
-- Group all of the related genes --
grouped_genes = GROUP related_genes BY name; 
-- Count how many time a gene is related and divide by total samples --
gene_score = FOREACH grouped_genes GENERATE $0, (DOUBLE)COUNT($1)/(DOUBLE)sample_count.count;
-- Need to cast to (DOUBLE) otherwise results get rounded down to 0 --
STORE gene_score INTO '$output' USING PigStorage(',');
