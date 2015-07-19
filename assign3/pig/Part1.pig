register Part1.jar
samples = LOAD '$input' USING PigStorage(',');
maxGenes = FOREACH samples GENERATE $0, Part1( (bag{tuple()}) TOBAG($1..) );
STORE maxGenes INTO '$output' USING PigStorage(',');
