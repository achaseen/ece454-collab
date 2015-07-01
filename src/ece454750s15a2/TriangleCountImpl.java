/**
 * ECE 454/750: Distributed Computing
 *
 * Code written by Wojciech Golab, University of Waterloo, 2015
 *
 * IMPLEMENT YOUR SOLUTION IN THIS FILE
 *
 */

package ece454750s15a2;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TriangleCountImpl {
    private byte[] input;
    private int numCores;

    public TriangleCountImpl(byte[] input, int numCores) {
	    this.input = input;
	    this.numCores = numCores;
    }

    public List<String> getGroupMembers() {
	    ArrayList<String> ret = new ArrayList<String>();
	    ret.add("dmjmasse");
		ret.add("m5ji");
		ret.add("achaseen");
	    return ret;
    }
	
    public List<Triangle> enumerateTriangles() throws IOException {
	    // this code is single-threaded and ignores numCores
    ExecutorService pool = Executors.newFixedThreadPool(numCores);
        long startTime = System.currentTimeMillis();
        ArrayList<ArrayList<Integer>> adjacencyList = getAdjacencyList(input);
        long endTime = System.currentTimeMillis();
        long diffTime = endTime - startTime;
	    System.out.println("Lists obtained in " + diffTime);

	    ArrayList<Triangle> ret = new ArrayList<Triangle>();

        startTime = System.currentTimeMillis();
        
        for (int i=0; i < adjacencyList.size()-1; i++) {
        	class eachVertex implements Runnable {
			int i;
			ArrayList<Triangle> ret;
			ArrayList<ArrayList<Integer>> adjacencyList;

			public eachVertex(int i, ArrayList<Triangle> ret, ArrayList<ArrayList<Integer>> adjacencyList) {
			this.i = i;
			this.ret = ret;
			this.adjacencyList = adjacencyList;
			}
			public void run() {
			int vertex1 = i;
        	
        	for(int j=0; j < adjacencyList.get(i).size()-1; j++) {
        		int vertex2 = adjacencyList.get(i).get(j);
        		if (vertex1 < vertex2) {
        			int count = j+1;
        		
        			boolean same_vertex = true;
        			while(same_vertex) {
	        			int vertex3 = adjacencyList.get(i).get(count);
	        			if( adjacencyList.get(vertex2).contains(vertex3) ) {
	        				ret.add( new Triangle(vertex1, vertex2, vertex3) );
	        			}
	
	        			count++;
	        			if(count >= adjacencyList.get(i).size() ){
	        				same_vertex = false;
	        			}
	        		}
        		}
        	}
			}
		}
		pool.submit(new eachVertex(i, ret, adjacencyList));
        	
        }
		pool.shutdown();
		try {
            pool.awaitTermination(1,TimeUnit.DAYS);
        } catch (InterruptedException e) {
            System.out.println("Pool interrupted!");
            System.exit(1);
        }
        
        endTime = System.currentTimeMillis();
        diffTime = endTime - startTime;
	    System.out.println("Count obtained in " + diffTime);
	    
        return ret;
    }

    public ArrayList<ArrayList<Integer>> getAdjacencyList(byte[] data) throws IOException {
	ExecutorService pool = Executors.newFixedThreadPool(numCores);
	
	InputStream istream = new ByteArrayInputStream(input);
	BufferedReader br = new BufferedReader(new InputStreamReader(istream));
	String strLine = br.readLine();
	if (!strLine.contains("vertices") || !strLine.contains("edges")) {
	    System.err.println("Invalid graph file format. Offending line: " + strLine);
	    System.exit(-1);	    
	}
	String parts[] = strLine.split(", ");
	int numVertices = Integer.parseInt(parts[0].split(" ")[0]);
	int numEdges = Integer.parseInt(parts[1].split(" ")[0]);
	System.out.println("Found graph with " + numVertices + " vertices and " + numEdges + " edges");
 
	ArrayList<ArrayList<Integer>> adjacencyList = new ArrayList<ArrayList<Integer>>(numVertices);
	for (int i = 0; i < numVertices; i++) {
	    adjacencyList.add(new ArrayList<Integer>());
	}
	while ((strLine = br.readLine()) != null && !strLine.equals(""))   {
	//----------------------------------------------------------------------------------------------------------
		class eachLine implements Runnable {
			String parts[];
			String strLine;
			ArrayList<ArrayList<Integer>> adjacencyList;

			public eachLine(String parts[], String strLine, ArrayList<ArrayList<Integer>> adjacencyList) {
			this.parts = parts;
			this.strLine = strLine;
			this.adjacencyList = adjacencyList;
			}
		public void run() {
        parts = strLine.split(": ");
	    int vertex = Integer.parseInt(parts[0]);
	    if (parts.length > 1) {
		parts = parts[1].split(" +");
		for (String part: parts) {
		    adjacencyList.get(vertex).add(Integer.parseInt(part));
			//System.out.println("Found edge: " + vertex + "-" + part);
					}
				}
			}
		}
		pool.submit(new eachLine(parts, strLine, adjacencyList));
	}
	pool.shutdown();
	 try {
            pool.awaitTermination(1,TimeUnit.DAYS);
        } catch (InterruptedException e) {
            System.out.println("Pool interrupted!");
            System.exit(1);
        }
	//----------------------------------------------------------------------------------------------------------------
	br.close();
	return adjacencyList;
    }
}
