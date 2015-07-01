#!/bin/bash
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph100K_A.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph100K_A
echo ------
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph10K_A.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/empty
echo ------
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph1K_A.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/empty
echo ------
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph1M_A.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/empty
echo ------
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph100K_B.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph100K_B
echo ------
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph10K_B.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph10K_B
echo ------
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph1K_B.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph1K_B
echo ------
java -Xmx4096m -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph1M_B.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph1M_B
echo ------
java -Xmx4096m -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph100K_C.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph100K_C
echo ------
java -Xmx4096m -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph10K_C.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph10K_C
echo ------
java -cp a2.jar ece454750s15a2.TriangleCount -ncores 2 -if inputs/graph1K_C.txt -of output.txt
perl sorted/sort.pl output.txt > new_list
echo ------
diff new_list sorted/graph1K_C
echo ------
