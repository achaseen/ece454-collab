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
    	
    	List<Triangle> ret = Collections.synchronizedList(new ArrayList<Triangle>());
    	
        if(numCores < 2) {
        	long startTime = System.currentTimeMillis();
            ArrayList<HashSet<Integer>> adjacencyList = getSingleAdjacencyList(input);
            long endTime = System.currentTimeMillis();
            long diffTime = endTime - startTime;
    	    System.out.println("Lists obtained in " + diffTime);    	    

            startTime = System.currentTimeMillis();
	        for (int i=0; i < adjacencyList.size()-1; i++) {
	        	       	
	            int vertex1 = i;
	            if(adjacencyList.get(vertex1).size() > 1){
	            	Object[] vertex1Array = adjacencyList.get(i).toArray();
	            	for(int j=0; j < vertex1Array.length; j++) {
	        		
	            		int vertex2 = (Integer)vertex1Array[j];
	            		if(adjacencyList.get(vertex2).size() > 1){
	            			if (vertex1 < vertex2) {
	        			
	            				for(int count=j+1; count<vertex1Array.length; count++) {        			
	            					int vertex3 = (Integer)vertex1Array[count];
	            					if(vertex1 < vertex3){
	            						if( adjacencyList.get(vertex2).contains(vertex3) ) {
	            							if( vertex2 < vertex3 ) {
	            								ret.add( new Triangle(vertex1, vertex2, vertex3) );
	            							}else{
	            								ret.add( new Triangle(vertex1, vertex3, vertex2) );
	            							}
	            						}
	            					}        			
	            				}
	            			}
	            		}
	            	}
	            }
	        }
	        endTime = System.currentTimeMillis();
	        diffTime = endTime - startTime;
		    System.out.println("Count obtained in " + diffTime);
	    
    	}else{
    		// parallelized version
    		ExecutorService pool = Executors.newFixedThreadPool(numCores);
            long startTime = System.currentTimeMillis();
            ConcurrentHashMap<Integer,HashSet<Integer>> adjacencyList = getParallelAdjacencyList(input);
            long endTime = System.currentTimeMillis();
            long diffTime = endTime - startTime;
    	    System.out.println("Lists obtained in " + diffTime);

            startTime = System.currentTimeMillis();
            
            for (int i=0; i < adjacencyList.size()-1; i++) {
            	class eachVertex implements Runnable {
            		int i;
            		List<Triangle> ret;
            		ConcurrentHashMap<Integer,HashSet<Integer>> adjacencyList;

            		public eachVertex(int i, List<Triangle> ret, ConcurrentHashMap<Integer,HashSet<Integer>> adjacencyList) {
            			this.i = i;
            			this.ret = ret;
            			this.adjacencyList = adjacencyList;
            		}
	    			public void run() {
	    				int vertex1 = i;
	    				
	    				if(adjacencyList.get(vertex1).size() > 1){
	    					Object[] vertex1Array = adjacencyList.get(i).toArray();
	    	            	for(int j=0; j < vertex1Array.length; j++) {
	    	            		int vertex2 = (Integer)vertex1Array[j];
	    	            		if(adjacencyList.get(vertex2).size() > 1){
	    	            			if (vertex1 < vertex2) {
	    	        			
	    	            				for(int count=j+1; count<vertex1Array.length; count++) {        			
	    	            					int vertex3 = (Integer)vertex1Array[count];
	    	            					if(vertex1 < vertex3){
	    	            						if( adjacencyList.get(vertex2).contains(vertex3) ) {
	    	            							if( vertex2 < vertex3 ) {
	    	            								ret.add( new Triangle(vertex1, vertex2, vertex3) );
	    	            							}else{
	    	            								ret.add( new Triangle(vertex1, vertex3, vertex2) );
	    	            							}
	    	            						}
	    	            					}        			
	    	            				}
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
    	}
	    
        return ret;
    }

    public ArrayList<HashSet<Integer>> getSingleAdjacencyList(byte[] data) throws IOException {
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
 
	    ArrayList<HashSet<Integer>> adjacencyList = new ArrayList<HashSet<Integer>>(numVertices);
		
        while ((strLine = br.readLine()) != null && !strLine.equals(""))   {
        	adjacencyList.add(new HashSet<Integer>());
	        parts = strLine.split(": ");
	        int vertex = Integer.parseInt(parts[0]);
	        if (parts.length > 1) {
		        parts = parts[1].split(" ");
		        for (String part: parts) {
		        	adjacencyList.get(vertex).add(Integer.parseInt(part));
		        }
	        }
        
	    }
	    br.close();

	    return adjacencyList;
    }
    
    public ConcurrentHashMap<Integer,HashSet<Integer>> getParallelAdjacencyList(byte[] data) throws IOException {
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
     
    	ConcurrentHashMap<Integer,HashSet<Integer>> adjacencyList = new ConcurrentHashMap<Integer,HashSet<Integer>>(numVertices);
		
    	for (int i = 0; i < numVertices; i++) {
    	    adjacencyList.put(i, new HashSet<Integer>());
    	}
		
    	while ((strLine = br.readLine()) != null && !strLine.equals(""))   {
    	//----------------------------------------------------------------------------------------------------------
    		class eachLine implements Runnable {
    			String parts[];
    			String strLine;
    			
    			ConcurrentHashMap<Integer,HashSet<Integer>> adjacencyList;

    			public eachLine(String parts[], String strLine, ConcurrentHashMap<Integer,HashSet<Integer>> adjacencyList) {
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
