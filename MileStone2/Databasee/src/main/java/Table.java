package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

public class Table implements Serializable {
	Vector<Page> tablePages;
	String tableName;
	int maxRows;
	int pageCount = 0;
	String clusteringKey;
    Vector<OctTree> trees=new Vector<OctTree>();

	public Table(int maxRows, String tableName) throws IOException {
		this.maxRows = maxRows;
		this.tableName = tableName;
		tablePages = new Vector<Page>();
		// tablePages.add(new Page(maxRows, this, ++pageCount));//ktbin laa
		// elmfrod mn3mlsh ela lma ygy awl whd
		clusteringKey = clusteringKeyValue(tableName);

	}

	public void printTable() throws DBAppException, ClassNotFoundException, IOException {
		if (this.tablePages.isEmpty()) {
			throw new DBAppException("Table is empty");
		}

		for (Page page : this.tablePages) {
			page.rows = Table.deserializable(this.tableName, page.pageNumber);
			System.out.println(page.pageNumber);
			page.printRecords();
		}
		for (Page page : this.tablePages) {

			Table.serializeObject(page.rows, this.tableName, page.pageNumber);
			page.rows = null;
		}
	}

	// gets clustering key of table
	public static String clusteringKeyValue(String tableName)// tested sh8ala
			throws IOException {
		String clusteringKeyValue = "";
		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String current = "";
		while ((current = br.readLine()) != null) {
			String[] line = current.split(",");
			// for (int i = 0; i < line.length; i += 7)
			// {
			String name = ((String) line[0]).replaceAll("\\s", "");
			String input = ((String) line[3]).replaceAll("\\s", "");

			if (name.equals(tableName)) {
				if ((input.toString()).equalsIgnoreCase("True")) {
					clusteringKeyValue = ((String) line[1]).replaceAll("\\s", "");
					br.close();
					return clusteringKeyValue;
				}
			}
			// }
		}
		br.close();
		return clusteringKeyValue;

	}

	// validates data then finds page to insert into
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			clusteringKey = clusteringKeyValue(strTableName);

			if (!htblColNameValue.containsKey(clusteringKey))
				throw new DBAppException("There is no clustering key");
			validateData(htblColNameValue);
			Tuples value = new Tuples(htblColNameValue, clusteringKey);
			// if table is empty
			if (this.tablePages.isEmpty()) {
				tablePages.add(new Page(maxRows, this, 1));
				tablePages.get(0).insertNewRecord(htblColNameValue);

			} else {

				Page page = getPageForValue(value);

				page.rows = Table.deserializable(this.tableName, page.pageNumber);
				Tuples tuple = new Tuples(htblColNameValue, clusteringKey);
				if (page.rows != null && page.isExist(tuple))
					throw new DBAppException("Clustering Key already Exists!");
				page.insertNewRecord(htblColNameValue);
				page = null;
			}
		} catch (ClassNotFoundException | ParseException | IOException e) {
			throw new DBAppException();
		}

	}

	public void updateInTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			if (htblColNameValue.containsKey(clusteringKeyValue(strTableName)))
				throw new DBAppException("No clustering key can be included in updated values");
			validateData(htblColNameValue);
			// if table is empty
			if (this.tablePages.isEmpty()) {
				throw new DBAppException("The table is empty, no tuples to update");
			}
			Object clusteringkeywithType = changeClusterKey(strTableName, clusteringKey, strClusteringKeyValue);
			htblColNameValue.put(clusteringKey, clusteringkeywithType);

			Tuples row = new Tuples(htblColNameValue, clusteringKey);
			Page page = getPageForUpdate(row);
			page.rows = Table.deserializable(this.tableName, page.pageNumber);

			page.updateExistingRecord(htblColNameValue, clusteringKey);
		} catch (ParseException | IOException | ClassNotFoundException e) {
			throw new DBAppException();
		}
	}

	public void deleteFromTable(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			if (htblColNameValue.isEmpty()) {
				this.tablePages.clear();
				removeEmptyPages(this.tablePages);
				return;
			}
			if (this.tablePages.isEmpty()) {
				throw new DBAppException("Table is empty");
			}
			validateData(htblColNameValue);
			if (htblColNameValue.containsKey(clusteringKey)) {
				Tuples row = new Tuples(htblColNameValue, clusteringKey);
				Page page = getPageForUpdate(row);

				page.rows = Table.deserializable(this.tableName, page.pageNumber);

				page.deleteRecordWithClusteringKey(htblColNameValue);
			} else {

				Iterator<Page> iterator = this.tablePages.iterator();
				while (iterator.hasNext()) {
					Page page = iterator.next();
					page.rows = Table.deserializable(this.tableName, page.pageNumber);
					page.deleteRecord(htblColNameValue);
					

				}

			}
			removeEmptyPages(this.tablePages);
		} catch (ParseException | IOException | ClassNotFoundException e) {
			throw new DBAppException();
		}

	}

	public void removeEmptyPages(Vector<Page> pages) throws ClassNotFoundException, IOException {
		ArrayList indecies = new ArrayList();
		Iterator<Page> iterator = pages.iterator();
		while (iterator.hasNext()) {
		    Page page = iterator.next();
		    page.rows = Table.deserializable(this.tableName, page.pageNumber);
		    if (page == null) {
		        continue;
		    }
		    if (page.rows == null || page.rows.size() == 0) {
		        iterator.remove();
		        indecies.add(page.pageNumber);
		    } else {
		        Table.serializeObject(page.rows, this.tableName, page.pageNumber);
		        page.rows = null;
		    }
		}
		updateIndex();
		renameAndDelete();
	}
  private void updateIndex() {
   for(Page page:this.tablePages)
	
	 
	 for (OctTree t : this.trees) {
					
					
	}
		
	}

	private void renameAndDelete() throws ClassNotFoundException, IOException {
		int lastFilenum;
		for (int i = 0; i < tablePages.size(); i++) {
			Page page = tablePages.get(i);
			page.rows = Table.deserializable(this.tableName, page.pageNumber);
			page.pageNumber = i + 1;
			Table.serializeObject(page.rows, this.tableName, page.pageNumber);
			page.rows = null;
			
			
		}
		File file;
		String fileName;
		if (tablePages.isEmpty()) {
			lastFilenum = 1;
			fileName = "src/main/resources/data/" + this.tableName + "/pages/" + this.tableName + "_" + lastFilenum
					+ ".ser";
			file = new File(fileName);

		} else {
			lastFilenum = (tablePages.get(tablePages.size() - 1).pageNumber) + 1;
			fileName = "src/main/resources/data/" + this.tableName + "/pages/" + this.tableName + "_" + lastFilenum
					+ ".ser";
			file = new File(fileName);
		}
		while (file.exists()) {
			file.delete();
			lastFilenum++;
			fileName = "src/main/resources/data/" + this.tableName + "/pages/" + this.tableName + "_" + lastFilenum
					+ ".ser";
			file = new File(fileName);
		}

	}

	public static Object changeClusterKey(String strTableName, String clusteringKey, String strClusteringKeyValue)
			throws IOException, ParseException, DBAppException {
		ArrayList data = readCsvCKDataType(strTableName, clusteringKey);
		// System.out.println(data.get(0));
		String s = (String) data.get(0);
		switch (s) {
		case "java.lang.Integer":
			try {
				return Integer.parseInt(strClusteringKeyValue);
			} catch (Exception e) {
				throw new DBAppException("Can't turn this Clustering Key Value to the " + s + " data type.");
			}
		case "java.lang.Double":
			try {
				return Double.parseDouble(strClusteringKeyValue);
			} catch (Exception e) {
				throw new DBAppException("Can't turn this ClusteringKey Value to the " + s + " data type.");
			}
		case "java.lang.String":
			try {
				return strClusteringKeyValue;
			} catch (Exception e) {
				throw new DBAppException("Can't turn this ClusteringKey Value to the " + s + " data type.");
			}
		case "java.util.Date":
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date date = dateFormat.parse(strClusteringKeyValue);
				return date;
			} catch (Exception e) {
				throw new DBAppException("Can't turn this ClusteringKey Value to the " + s + " data type.");
			}
		}

		return -1;
	}

	public boolean checkIfExists(String tableName) throws FileNotFoundException, IOException {
		boolean isExists = false;
		try (BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/" + "metadata.csv"))) {
			String line;

			while ((line = reader.readLine()) != null) {
				String[] columns = line.split(",");
				if (columns[0].trim().equals(tableName)) {
					isExists = true;
					break;
				}
			}
			reader.close();
			return isExists;
		}
	}

	public Page getPageForUpdate(Tuples value) throws IOException, DBAppException {
		for (Page page : this.tablePages) {
			// System.out.println(page.min.tuple.get("id"));
			// System.out.println(value.primaryKey);
			if (page.min.compareTo(value) <= 0 && (page.max.compareTo(value) >= 0)) {

				return page;
			}
		}
		throw new DBAppException("There is no entry with this Key");

	}

	public Page getPageForValue(Tuples value) throws IOException, ClassNotFoundException {
		for (Page page : this.tablePages) {
			if (page.min.compareTo(value) <= 0 && (page.max.compareTo(value) >= 0)) {

				return page;
			}

			if (page.min.compareTo(value) >= 0) {

				if (page.pageNumber != 1) {
					Vector lastpage = deserializable(this.tableName, page.pageNumber - 1);
					if (lastpage.size() != maxRows)
						return this.tablePages.get(page.pageNumber - 2);
					serializeObject(lastpage, this.tableName, page.pageNumber - 1);
					lastpage = null;
					return page;
				} else
					return page;

			}

		}

		if ((this.tablePages.get(this.tablePages.size() - 1).max).compareTo(value) < 0) {
			Vector lastpage = deserializable(this.tableName, this.tablePages.size());
			if (lastpage.size() == maxRows) {
				serializeObject(lastpage, this.tableName, this.tablePages.size());
				lastpage = null;
				this.tablePages.add(new Page(maxRows, this, this.tablePages.size() + 1));// fulll new page
				serializeObject(this.tablePages.get(this.tablePages.size() - 1).rows, this.tableName,
						this.tablePages.size());
			}
			return this.tablePages.get(this.tablePages.size() - 1);// add
																	// directly
		}
		return null;
	}

	public static void serializeObject(Vector t, String tableName, int pageNumber) throws IOException {
		String fileName = "src/main/resources/data/" + tableName + "/pages/" + tableName + "_" + pageNumber + ".ser";

		FileOutputStream fileOut = new FileOutputStream(fileName);

		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);

		try {
			objOut.writeObject(t);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		objOut.close();
		fileOut.close();

	}

	public static Vector deserializable(String tableName, int pageN) throws IOException, ClassNotFoundException {
		String fileName = "src/main/resources/data/" + tableName + "/pages/" + tableName + "_" + pageN + ".ser";
		Vector p = null;
		File file = new File(fileName);
		if (file.exists()) {
			FileInputStream fileStream = new FileInputStream(fileName);
			ObjectInputStream is = new ObjectInputStream(fileStream);
			p = (Vector) is.readObject();
			fileStream.close();
			is.close();
			return p;

		}
		return p;
	}

	// validate data
	public void validateData(Hashtable<String, Object> row) throws DBAppException, IOException, ParseException {

		for (Map.Entry<String, Object> col : row.entrySet()) {
			ArrayList<Object> metacoloumn = readCsvCKDataType(this.tableName, col.getKey());
			if (metacoloumn.size() == 0)// mfish coloumn fe eltable blasm dh
				throw new DBAppException("the coloumn name" + col.getKey() + " is not correct");
			Object val = col.getValue();
			String type = checkType(val);
			if (!type.equals(metacoloumn.get(0)))
				throw new DBAppException("Type of coloumn" + col.getKey() + " is not correct");

			switch (type) {
			case "java.lang.Integer":
				if (((Integer) val).compareTo((Integer) Integer.parseInt((String) metacoloumn.get(5))) > 0
						|| ((Integer) val).compareTo((Integer) Integer.parseInt((String) metacoloumn.get(4))) < 0)
					throw new DBAppException("out of bounds value for column " + col.getKey());
				break;
			case "java.lang.Double":
				if (((Double) val).compareTo((Double) Double.parseDouble((String) metacoloumn.get(5))) > 0
						|| ((Double) val).compareTo((Double) Double.parseDouble((String) metacoloumn.get(4))) < 0)
					throw new DBAppException("out of bounds value for column " + col.getKey());
				break;
			case "java.lang.String":
				if (((String) val).compareTo(((String) metacoloumn.get(5))) > 0
						|| ((String) val).compareTo(((String) metacoloumn.get(4))) < 0)
					throw new DBAppException("out of bounds value for column " + col.getKey());
				break;
			case "java.util.Date":
				if (((Date) val).compareTo(new SimpleDateFormat("yyyy-MM-dd").parse((String) metacoloumn.get(5))) > 0
						|| ((Date) val)
								.compareTo(new SimpleDateFormat("yyyy-MM-dd").parse((String) metacoloumn.get(4))) < 0)
					throw new DBAppException("out of bounds value for column " + col.getKey());
				break;

			default:
				throw new DBAppException("unsupported data type for column " + col.getKey());
			}
		}

	}

	public String checkType(Object key) throws ParseException {

		String type = key.getClass().toString();

		String typeString = "";
		if (type.substring(16).equals("Integer")) {
			typeString = "java.lang.Integer";
		}
		if (type.substring(16).equals("Double")) {
			typeString = "java.lang.Double";
		}

		if (type.substring(16).equals("String")) {
			typeString = "java.lang.String";
		}
		if (type.substring(16).equals("Date")) {
			typeString = "java.util.Date";
		}
		return typeString;
	}
	public  void beforeIndexInsert(OctTree<Object> tree,String[] strarrColName) throws ClassNotFoundException, IOException, DBAppException{
		for (Page page : this.tablePages) {
			page.rows=deserializable(this.tableName,page.pageNumber);
			
			for (Tuples t : page.rows) {
			
					tree.octTreeIndex.insert(t.tuple.get(strarrColName[0]),t.tuple.get(strarrColName[1]), t.tuple.get(strarrColName[2]),page. pageNumber);
				
			}

			
			page.rows=null;
		}	
	}

	public static ArrayList readCsvCKDataType(String tablename, String colName) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/" + "metadata.csv"));
		String current = "";
		ArrayList con = new ArrayList();
		while ((current = br.readLine()) != null) {
			String[] line = current.split(",");

			if (line[0].equals(tablename) && (line[1].toString()).replaceAll("\\s", "").equals(colName)) {

				
				con.add((line[2].toString()).replaceAll("\\s", ""));
				con.add((line[3].toString()).replaceAll("\\s", ""));
				con.add((line[4].toString()).replaceAll("\\s", ""));
				con.add((line[5].toString()).replaceAll("\\s", ""));
				con.add((line[6].toString()).replaceAll("\\s", ""));
				con.add((line[7].toString()).replaceAll("\\s", ""));

			}
		}
		br.close();
		return con;

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {
		// Table d= new Table(4,"d");
		// Page a = new Page(4,"d",1);
		// Hashtable<String, Object> row1 = new Hashtable();
		// row1.put("id", new Integer( 7 ));
		// row1.put("name", new String("Zaky Noor" ) );
		// row1.put("gpa", new Double( 0.88 ) );
		//
		//
		// Tuples t1= new Tuples(row1,"id");
		// a.rows.add(0,t1);
		// a.min=t1;
		//
		//
		// Hashtable<String, Object> row2 = new Hashtable();
		// row2.put("id", new Integer( 10 ));
		// //row2.put("name", new String("gego badrawy" ) );
		// row2.put("gpa", new Double( 1.7 ) );
		//
		//
		// Tuples t2= new Tuples(row2,"id");
		// a.rows.add(1,t2);
		// a.max=t2;
		// Hashtable<String, Object> row3 = new Hashtable();
		// row3.put("id", new Integer( 13));
		// row3.put("name", new String("gego badrawy" ) );
		// row3.put("gpa", new Double( 1.7 ) );
		//
		//
		// Tuples t3= new Tuples(row3,"id");
		// a.rows.add(2,t3);
		// a.max=t3;
		// d.tablePages.set(0, a);
		// Page a2 = new Page(4,"d",2);
		// Hashtable<String, Object> row6 = new Hashtable();
		// row6.put("id", new Integer( 16));
		// row6.put("name", new String("gego badrawy" ) );
		// row6.put("gpa", new Double( 1.7 ) );
		// Tuples t6= new Tuples(row3,"id");
		// a2.rows.add(0,t6);
		// Hashtable<String, Object> row7 = new Hashtable();
		// row7.put("id", new Integer( 19));
		// row7.put("name", new String("gego badrawy" ) );
		// row7.put("gpa", new Double( 1.7 ) );
		// Tuples t7= new Tuples(row3,"id");
		// a2.rows.add(0,t7);
		//
		//
		// Hashtable<String, Object> row9 = new Hashtable();
		// row9.put("id", new Integer( 30));
		// row9.put("name", new String("gego badrawy" ) );
		// row9.put("gpa", new Double( 1.7 ) );
		// Tuples t9= new Tuples(row3,"id");
		// a2.rows.add(0,t9);

		Hashtable<String, Object> row11 = new Hashtable();
		row11.put("id", new Integer(3));
		row11.put("name", new String("gego badrawy"));
		row11.put("gpa", new Double(1.7));
		row11.put("date", new SimpleDateFormat("dd/mm/yyyy").parse(("1/3/2005")));
		// Tuples t11= new Tuples(row3,"id");
		// a2.rows.add(0,t11);
		//
		// Hashtable<String, Object> row8 = new Hashtable();
		// row8.put("id", new Integer( 2));
		// row8.put("name", new String("gego badrawy" ) );
		// row8.put("gpa", new Double( 1.7 ) );

		// a2.min=t6;
		// a2.max=t11;
		//
		// Tuples t8= new Tuples(row8,"id");
		// d.tablePages.add(1, a2);
		// System.out.print(d.getPageForValue(t8).pageNumber);

		// System.out.print(d.tablePages.get(0).rows.get(0).tuple);
		// Table.serializeObject(a2, d.tableName);
		// d.insertIntoTable("d", row8);
		// Page p=Table.deserializable(d.tableName,2);
		// System.out.print(p.rows.get(0).tuple);
		// System.out.print(d.checkIfExists("d"));
//		Table tohfa = new Table(250, "tohfa");
//		ArrayList b = new ArrayList();
//		b = (Table.readCsvCKDataType("tohfa", "name"));
//		Object val = row11.get("gpa");
//		String type = tohfa.checkType(b.get(5));
//		//System.out.print("gego".compareTo((String) b.get(5)));
//	     tohfa.validateData(row11);
	}

}