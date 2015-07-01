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
	    return ret;
    }
    public List<Triangle> enumerateTriangles() throws IOException {
	    // this code is single-threaded and ignores numCores
    
        long startTime = System.currentTimeMillis();
        ArrayList<ArrayList<Integer>> adjacencyList = getAdjacencyList(input);
        long endTime = System.currentTimeMillis();
        long diffTime = endTime - startTime;
	    System.out.println("Lists obtained in " + diffTime);

	    ArrayList<Triangle> ret = new ArrayList<Triangle>();

        startTime = System.currentTimeMillis();
        
        for (int i=0; i < adjacencyList.size()-1; i++) {
        	
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
        
        endTime = System.currentTimeMillis();
        diffTime = endTime - startTime;
	    System.out.println("Count obtained in " + diffTime);
	    
        return ret;
    }

    public ArrayList<ArrayList<Integer>> getAdjacencyList(byte[] data) throws IOException {
	    InputStream istream = new ByteArrayInputStream(data);
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
	
        while ((strLine = br.readLine()) != null && !strLine.equals(""))   {
        	adjacencyList.add(new ArrayList<Integer>());
	        parts = strLine.split(": ");
	        int vertex = Integer.parseInt(parts[0]);
	        if (parts.length > 1) {
		        parts = parts[1].split(" ");
                if (parts.length == 0 || parts.length == 1) {
                	//do nothing
                } else if (parts.length == 2 && (
                		(Integer.parseInt(parts[0]) < vertex &&
                				adjacencyList.get(Integer.parseInt(parts[0])).size() < 2) ||
                		(Integer.parseInt(parts[1]) < vertex &&
                		        adjacencyList.get(Integer.parseInt(parts[1])).size() < 2) ) ) {
                	//do nothing
                } else {
		            for (String part: parts) {
		                adjacencyList.get(vertex).add(Integer.parseInt(part));
		            }
                }
	        }
	    }
	    for (int i = 0; i < numVertices; i++) {
	       Collections.sort(adjacencyList.get(i));
	    }
	    br.close();

        //for (int i = 0; i< adjacencyList.size(); i++ ) {
         //  System.out.println(adjacencyList.get(i));
        //}
	    return adjacencyList;
    }
}
