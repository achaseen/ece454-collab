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
        ArrayList<ArrayList<Integer>> edgeList = getEdgeList(input);
        ArrayList<ArrayList<Integer>> adjacencyList = getAdjacencyList(input);
        long endTime = System.currentTimeMillis();
        long diffTime = endTime - startTime;
	    System.out.println("Lists obtained in " + diffTime/1000 + "s");

	    ArrayList<Triangle> ret = new ArrayList<Triangle>();

	    long loops1 = 0, loops2 = 0, loops3 = 0;
        startTime = System.currentTimeMillis();
        int numEdges = edgeList.size();
        for (int i = 0; i < numEdges-1; i++) {
        	loops1++;
        	boolean vertex1_match = true;
        	ArrayList<Integer> edge1 = edgeList.get(i);
        	int vertex1 = edge1.get(0);
        	int vertex2 = edge1.get(1);
        	
        	int count = i+1;
        	while(vertex1_match){
        		loops2++;
        		ArrayList<Integer> edge2 = edgeList.get(count);
        		if( edge2.get(0) == vertex1 ) {
        			int vertex3 = edge2.get(1);
        			// Two edges share a common vertex
        			// Find matching edge
        			if( adjacencyList.get(vertex2).contains(vertex3) ) {
        				if(vertex2<vertex3) {
        					ret.add( new Triangle(vertex1, vertex2, vertex3) );
        				}else{
        					ret.add( new Triangle(vertex1, vertex3, vertex2) );
        				}
        			}
        		}else{
        			vertex1_match = false;
        			break;
        		}
        		count++;
        		if(count+1 >= numEdges ){
        			vertex1_match = false;
        		}
        	}
        }
        
        endTime = System.currentTimeMillis();
        diffTime = endTime - startTime;
	    System.out.println("Count obtained in " + diffTime/1000 + "s");
	    System.out.println("loop1: " + loops1 + " loops2: " + loops2 + " loops3: " +loops3);
	    
        return ret;
    }

    public ArrayList<ArrayList<Integer>> getEdgeList(byte[] data) throws IOException {
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

	    ArrayList<ArrayList<Integer>> edgeList = new ArrayList<ArrayList<Integer>>(numEdges);
	    
        while ((strLine = br.readLine()) != null && !strLine.equals(""))   {
	        parts = strLine.split(": ");
	        int vertex1 = Integer.parseInt(parts[0]);
	        if (parts.length > 1) {
		        parts = parts[1].split(" ");
		        if ( parts.length > 1 ) {
		        	for (String part: parts) {
		        		if( vertex1 < Integer.parseInt(part) ) {
		        			ArrayList<Integer> myList = new ArrayList<Integer>();
		        			myList.add(vertex1);
		        			myList.add(Integer.parseInt(part));
		        			edgeList.add(myList);
		        		}
		        	}
		        }
	        }
        }
	    br.close();
	    return edgeList;
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
