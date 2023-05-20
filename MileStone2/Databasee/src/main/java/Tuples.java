package main.java;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;


public class Tuples implements Comparable<Tuples> , Serializable {

    Hashtable<String, Object> tuple ;
	Object primaryKey;
	
	public Tuples (Hashtable <String,Object>input , String clusteringKey ) throws ParseException {
	
	tuple=input;
	primaryKey=input.get(clusteringKey);
//	System.out.println(primaryKey);
	}
	
	//things related to tuples
	//check type of cluster key
	public  String checkType(Object key ) throws ParseException{
		
	        String type = key.getClass().toString();
	    	//System.out.println(primaryKey);

	       
	        String typeString="";
	       if(type.substring(16).equals("Integer")) {
	        	typeString="java.lang.Integer";
	        	}
	        if(type.substring(16).equals("Double")) {
	        	typeString="java.lang.Double";}
	        
	        if(type.substring(16).equals("String")) {
	        	typeString="java.lang.String";
	         	}
	         if(type.substring(16).equals("Date") ){
	        	 typeString="java.util.Date";
		        }
	        return typeString; 
	    }
	//compare to method
	@Override
	public int compareTo(Tuples o) {
		 String dataType;
		try {
			dataType = checkType(o.primaryKey);
			//System.out.println(dataType);
		
		switch(dataType) {
		case "java.lang.Integer":
			 return ((Integer) this.primaryKey).compareTo((Integer) o.primaryKey );  
		case "java.lang.Double":
			return ((Double) this.primaryKey).compareTo((Double) o.primaryKey );
		case "java.lang.String":
			return ((String) this.primaryKey).compareTo((String) o.primaryKey );
		case "java.util.Date":
			return ((Date) this.primaryKey).compareTo((Date) o.primaryKey );
		
		}} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;
	}
public static void main (String[]args) throws ParseException {

	}
	


}