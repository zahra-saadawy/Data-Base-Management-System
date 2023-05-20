package main.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class DBApp {
	static String csvFilePath = "src/main/resources/metadata.csv"; // specify
																	// the file
	// path
	Vector<Table> tableList = new Vector<Table>();

	public static void printTableContent(String tableName)
			throws ClassNotFoundException, IOException, DBAppException {
		boolean isExists = false;
		try (BufferedReader reader = new BufferedReader(new FileReader(
				"src/main/resources/" + "metadata.csv"))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] columns = line.split(",");
				if (columns[0].trim().equals(tableName)) {
					isExists = true;

					break;
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (!isExists) {
			throw new DBAppException("Table not found");
		}

		Table table = deserialize(tableName);
		if (table.tablePages.size() != 0)
			table.printTable();
		else
			System.out.println("Table is Empty!");
		serialize(table);
	}

	public void createIndex(String strTableName, String[] strarrColName)
			throws DBAppException {
		Object[] min = new Object[3];
		Object[] max = new Object[3];

		if (!checkTable(strTableName))
			throw new DBAppException("Table name is not exist in the database");

		try {
			validateIndexColms(strTableName, strarrColName);
		} catch (ClassNotFoundException | IOException e) {
			throw new DBAppException(e.getMessage());

		}
		int i = 0;
		for (String s : strarrColName) {
			ArrayList a;
			
			
			try {
				a = Table.readCsvCKDataType(strTableName, s);
				min[i] = Table.changeClusterKey(strTableName,s,(String)a.get(4));
				max[i++] = Table.changeClusterKey(strTableName,s,(String)a.get(5));
			} catch (IOException | ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		}
	
		OctTree<Object> tree = new OctTree<Object>(strarrColName[0],strarrColName[1],strarrColName[2],min[0], min[1],
				min[2], max[0], max[1], max[2],getconfigindex());
		// set coloumns to yes indexed and what index name and type
             setIndexTrue(strTableName,strarrColName);
        //check if table not empty we have to insert what have inserted  before
             try {
				Table t=deserialize(strTableName);
				t.trees.add(tree);
				t.beforeIndexInsert(tree,strarrColName);
				serialize(t);
				t=null;
				
			} catch (ClassNotFoundException | IOException e) {
				throw new DBAppException(e.getMessage());
			}
		tree.octTreeIndex.print();

	}
	public static int getconfigindex() {
		int noOfentries;

		Properties config = new Properties();
		try {
			InputStream input = new FileInputStream(new File(
					"src/main/resources/DBApp.config"));
			config.load(input);
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		noOfentries = Integer.parseInt(config
				.getProperty("MaximumKeysCountinIndexBucket"));

		return noOfentries;
	}
	public static void setIndexTrue(String strTableName, String[] strarrColName) {
		String filePath = "src/main/resources/metadata.csv";
		// specify the line number to change

		try {
			// create a new file reader
			FileReader fr = new FileReader(filePath);
			BufferedReader br = new BufferedReader(fr);

			// create a new file writer
			FileWriter fw = new FileWriter(filePath + ".tmp");
			BufferedWriter bw = new BufferedWriter(fw);

			// loop through each line in the file
			String line;
			int i = 1;
			while ((line = br.readLine()) != null) {
				// check if this is the line to change
				String[] lines = line.split(",");

				if (lines[0].equals(strTableName)
						&& ((lines[1].toString()).replaceAll("\\s", "").equals(
								strarrColName[0])
								|| (lines[1].toString()).replaceAll("\\s", "")
										.equals(strarrColName[1]) || (lines[1]
									.toString()).replaceAll("\\s", "").equals(
								strarrColName[2]))) {
					// write the new values to the line
					lines[4]=""+strarrColName[0]+strarrColName[1]+strarrColName[2]+"Index";
					lines[5]="Octree";
					String newLine = String.join(",", lines);
					bw.write(newLine);
					bw.newLine();
				} else {
					// write the original line to the file
					bw.write(line);
					bw.newLine();
				}
				i++;
			}

			// close the file reader and writer
			br.close();
			fr.close();
			bw.close();
			fw.close();

			// delete the original file
			File oldFile = new File(filePath);
			oldFile.delete();

			// rename the temporary file to the original file name
			File newFile = new File(filePath + ".tmp");
			newFile.renameTo(oldFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void validateIndexColms(String strTableName, String[] strarrColName)
			throws DBAppException, IOException, ClassNotFoundException {
		if (strarrColName.length != 3)
			throw new DBAppException("Please enter a table with 3 columns");

		Table t = (Table) deserialize(strTableName);
		for (String s : strarrColName) {
			ArrayList a = t.readCsvCKDataType(strTableName, s);

			if (a == null || a.size() == 0)
				throw new DBAppException(
						"No column with this name in this Table");
			else

			if (!a.get(2).equals("null"))
				throw new DBAppException("there is exist index on this coloumn");
		}
		serialize(t);
		t = null;

	}

	public static void main(String[] args) throws DBAppException,
			ClassNotFoundException, IOException, ParseException {
		// ClassNotFoundException, IOException, ParseException {
		// init();
		String strTableName = "birdsEnhanced";
		DBApp dbApp = new DBApp();
		dbApp.init();
		// // // System.out.println("Before:");
		// // // DBApp.printTableContent(strTableName);
		// // // String strTableName1 = "Teacher2";
		Hashtable htblColNameType1 = new Hashtable();
		Hashtable htblColNameMin1 = new Hashtable();
		Hashtable htblColNameMax1 = new Hashtable();
		htblColNameType1.put("id", "java.lang.Integer");
		htblColNameType1.put("name", "java.lang.String");
		htblColNameType1.put("gpa", "java.lang.Double");
		htblColNameType1.put("phone", "java.lang.Integer");
		htblColNameType1.put("date", "java.util.Date");
		htblColNameType1.put("email", "java.lang.String");
		htblColNameMin1.put("id", "0");
		htblColNameMin1.put("name", "a");
		htblColNameMin1.put("gpa", "1.0");
		htblColNameMin1.put("phone", "0");
		htblColNameMin1.put("date", "1903-03-01");
		htblColNameMin1.put("email", "a");

		htblColNameMax1.put("id", "30");
		htblColNameMax1.put("name", "zzzzzz");
		htblColNameMax1.put("gpa", "5.0");
		htblColNameMax1.put("phone", "99999999");
		htblColNameMax1.put("date", "2022-12-31");
		htblColNameMax1.put("email", "zzzzzz");
		
		// Hashtable htblColNameType1 = new Hashtable();
		// Hashtable htblColNameMin1 = new Hashtable();
		// Hashtable htblColNameMax1 = new Hashtable();
		// htblColNameType1.put("id", "java.lang.Integer");
		// htblColNameType1.put("datee", "java.util.Date");
		// htblColNameType1.put("gpa", "java.lang.Double");
		// htblColNameMin1.put("id", 0);
		// htblColNameMin1.put("datee", new
		// SimpleDateFormat("yyyy-MM-dd").parse("1903-03-01"));
		// htblColNameMin1.put("gpa", new Double(1.0));
		//
		// htblColNameMax1.put("id", 11);
		// htblColNameMax1.put("datee", new
		// SimpleDateFormat("yyyy-MM-dd").parse("2222-12-12"));
		// htblColNameMax1.put("gpa", new Double(5.0));

	//dbApp.createTable(strTableName, "id", htblColNameType1,htblColNameMin1, htblColNameMax1);
		String[] a = { "name", "id", "gpa" };
	//dbApp.createIndex(strTableName, a);
		//dbApp.validateIndexColms(strTableName, a);
		 Hashtable<String, Object> rowww = new Hashtable();
		 rowww.put("name", "may");
//		 rowww.put("id",new Integer(15));
//		 rowww.put("gpa", new Double(5.0));
//		 
		 rowww.put("phone", new Integer(25999999));
		 rowww.put("date", new SimpleDateFormat("yyyy-MM-dd").parse("2003-12-12"));
		 rowww.put("email", "shiftawymail");
		// // System.out.print(rowww.get("gpa"));
		// ////
   dbApp.insertIntoTable(strTableName, rowww);
		// dbApp.updateTable(strTableName,"15",rowww);
	dbApp.deleteFromTable(strTableName, rowww);
         Table t=deserialize(strTableName);
        t.trees.get(0).octTreeIndex.print();
        // System.out.print( t.trees.get(0).octTreeIndex.checkExistsPoint(new OctPoint("diftawy",21,5.0,4),  t.trees.get(0).octTreeIndex.root));
//             System.out.println("Table 2");
//            t.trees.get(1).octTreeIndex.print();
		

		// // int index = dbApp.getIndexByName(dbApp.tableList, strTableName);
		// //
		// System.out.println(dbApp.tableList.get(index).tablePages.get(0).rows);
		// dbApp.deleteFromTable(strTableName, rowww);
		// // System.out.println("After:");
DBApp.printTableContent(strTableName);
		// // Hashtable<String, Object> edit = new Hashtable();
		// // edit.put("gpa", new Double(4.7));
		// // dbApp.updateTable("Test1", "4000" , edit);
		// // System.out.println(dbApp.tableList.get(0).tableName);
		// // System.out.print(dbApp.getconfig());
		// // dbApp.validateData(row11);
		// // ArrayList b=new ArrayList();
		// // b=(Table. readCsvCKDataType("tohfa","gpa"));
		// //
		// // for(int i=0;i<b.size();i++)
		// // System.out.print(b.get(i));
		// String tableName = "stun";
		//
		// Hashtable<String, String> htblColNameType = new Hashtable<String,
		// String>();
		// htblColNameType.put("id", "java.lang.String");
		// htblColNameType.put("first_name", "java.lang.String");
		// htblColNameType.put("last_name", "java.lang.String");
		// htblColNameType.put("dob", "java.util.Date");
		// htblColNameType.put("gpa", "java.lang.Double");
		//
		// Hashtable<String, String> minValues = new Hashtable<>();
		// minValues.put("id", "43-0000");
		// minValues.put("first_name", "AAAAAA");
		// minValues.put("last_name", "AAAAAA");
		// minValues.put("dob", "1990-01-01");
		// minValues.put("gpa", "0.7");
		//
		// Hashtable<String, String> maxValues = new Hashtable<>();
		// maxValues.put("id", "99-9999");
		// maxValues.put("first_name", "zzzzzz");
		// maxValues.put("last_name", "zzzzzz");
		// maxValues.put("dob", "2000-12-31");
		// maxValues.put("gpa", "5.0");
		// Hashtable<String, Object> rowww1 = new Hashtable();
		// rowww1.put("id", "50-11");
		// rowww1.put("first_name", "kitten");
		// rowww1.put("last_name", "moh");
		// rowww1.put("gpa", new Double(5));
		// rowww1.put("dob", new
		// SimpleDateFormat("yyyy-MM-dd").parse("1995-03-01"));
		// // dbApp.insertIntoTable(tableName, rowww1);
		// // dbApp.deleteFromTable(tableName, rowww1);
		// // DBApp.printTableContent(tableName);
		//
		// // dbApp.createTable(tableName, "id", htblColNameType, minValues,
		// // maxValues);
		// // String line;
		// // try (BufferedReader br = new BufferedReader(new
		// // FileReader("src/main/resources\\metadata.csv"))) {
		// //
		// // while ((line = br.readLine()) != null) {
		// //
		// // // use comma as separator
		// // String[] data = line.split(",");
		// //
		// // // do something with the data
		// // System.out.println("Column 1: " + data[6] + ", Column 2: " +
		// // data[7]);
		// // System.out.print(checkType(data[6]));
		// // }
		// //
		// // } catch (IOException e) {
		// // e.printStackTrace();
		// // }
		// // String tableName1 = "courses";
		// //
		// // Hashtable<String, String> htblColNameType11 = new
		// Hashtable<String,
		// // String>();
		// // htblColNameType11.put("date_added", "java.util.Date");
		// // htblColNameType11.put("course_id", "java.lang.String");
		// // htblColNameType11.put("course_name", "java.lang.String");
		// // htblColNameType11.put("hours", "java.lang.Integer");
		// //
		// //
		// // Hashtable<String, String> minValues1 = new Hashtable<>();
		// // minValues1.put("date_added", "1901-01-01");
		// // minValues1.put("course_id", "0000");
		// // minValues1.put("course_name", "AAAAAA");
		// // minValues1.put("hours", "1");
		// //
		// // Hashtable<String, String> maxValues1 = new Hashtable<>();
		// // maxValues1.put("date_added", "2020-12-31");
		// // maxValues1.put("course_id", "9999");
		// // maxValues1.put("course_name", "zzzzzz");
		// // maxValues1.put("hours", "24");
		// //
		// // // dbApp.createTable(tableName1, "date_added", htblColNameType11,
		// // minValues1, maxValues1);
		// // String tableName11 = "transcripts";
		//
		// // Hashtable<String, String> htblColNameType111 = new
		// Hashtable<String,
		// // String>();
		// // htblColNameType111.put("gpa", "java.lang.Double");
		// // htblColNameType111.put("student_id", "java.lang.String");
		// // htblColNameType111.put("course_name", "java.lang.String");
		// // htblColNameType111.put("date_passed", "java.util.Date");
		// //
		// // Hashtable<String, String> minValues11 = new Hashtable<>();
		// // minValues11.put("gpa", "0.7");
		// // minValues11.put("student_id", "43-0000");
		// // minValues11.put("course_name", "AAAAAA");
		// // minValues11.put("date_passed", "1990-01-01");
		// //
		// // Hashtable<String, String> maxValues11 = new Hashtable<>();
		// // maxValues11.put("gpa", "5.0");
		// // maxValues11.put("student_id", "99-9999");
		// // maxValues11.put("course_name", "zzzzzz");
		// // maxValues11.put("date_passed", "2020-12-31");
		//
		// // dbApp.createTable(tableName11, "gpa", htblColNameType111,
		// // minValues11, maxValues11);
		//
		// // InputStream input = new FileInputStream(new
		// // File("src/main/resources/DBApp.config"));
		//
		// // try {
		// // //insertCoursesRecords(dbApp,3);
		// //
		// //String table = "courses";
		//
	}

	public static void init() {
		String header = "Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max";
		String path = "src/main/resources/data";
		File file = new File(path);
		file.mkdir();
		// FileWriter fileWriter;
		// try {
		// fileWriter = new FileWriter(csvFilePath, true);
		// BufferedWriter bw = new BufferedWriter(fileWriter);
		// bw.write(header);
		// bw.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		try {
			// create metadata if not exists
			File metaData = new File("src/main/resources/metadata.csv");
			if (!metaData.exists()) {
				try {
					FileWriter filewrite = new FileWriter(metaData);
					BufferedWriter b = new BufferedWriter(filewrite);
					b.write("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max");
					b.newLine();
					b.flush();
					b.close();
					filewrite.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			// check if table already exists and not null
			if (checkTable(strTableName))
				throw new DBAppException(
						"Table name already exists in the database");
			if (strTableName == null || strTableName == "")
				throw new DBAppException("Table should have a name ");
			else {
				// check if no clustering key
				if (strClusteringKeyColumn == null)
					throw new DBAppException("Table should have clusteringKey");
				// check if datatype is correct and no min and max is empty
				Enumeration<String> colNames = htblColNameType.keys();
				while (colNames.hasMoreElements()) {
					String currColumn = colNames.nextElement();

					String checkType = htblColNameType.get(currColumn);

					if ((!checkType.equals("java.lang.Integer"))
							&& (!checkType.equals("java.lang.String"))
							&& (!checkType.equals("java.lang.Double"))
							&& (!checkType.equals("java.util.Date")))
						throw new DBAppException("Invalid Datatype for column");

					// if (htblColNameMax.get(currColumn) == null ||
					// htblColNameMin.get(currColumn) == null
					// || checkType != checkType(htblColNameMin.get(currColumn))
					// || checkType !=
					// checkType(htblColNameMax.get(currColumn)))
					// throw new
					// DBAppException("Invalid Datatype or empty column of "+currColumn);

					if (!htblColNameMin.containsKey(currColumn)
							|| !htblColNameMax.containsKey(currColumn)) {
						System.out
								.print(htblColNameMin.containsKey(currColumn));
						throw new DBAppException(
								"MIN AND MAX SHOULD NOT BE EMPTY!");

					}
					determineType(htblColNameMax.get(currColumn), checkType);
					determineType(htblColNameMin.get(currColumn), checkType);

				}
				// write to csv file
				colNames = htblColNameType.keys();
				while (colNames.hasMoreElements()) {
					boolean clustK;
					String currColumn = colNames.nextElement();
					if (currColumn.equals(strClusteringKeyColumn))
						clustK = true;
					else
						clustK = false;

					String inputLine = strTableName + ", " + currColumn + ", "
							+ htblColNameType.get(currColumn) + ", " + clustK
							+ ", " + null + ", " + null + ", "
							+ String.valueOf(htblColNameMin.get(currColumn))
							+ ", "
							+ String.valueOf(htblColNameMax.get(currColumn));
					// if
					// (!htblColNameType.get(currColumn).equals("java.util.Date"))
					// {
					// inputLine +=
					// String.valueOf(htblColNameMin.get(currColumn)) + ", "
					// + String.valueOf(htblColNameMax.get(currColumn));
					// } else {
					// inputLine += new
					// SimpleDateFormat("yyyy-MM-dd").format(htblColNameMin.get(currColumn))
					// + ", ";
					// inputLine += new
					// SimpleDateFormat("yyyy-MM-dd").format(htblColNameMax.get(currColumn));
					// }

					writeToCSV(inputLine);
				}

				Table newTable = new Table(getconfig(), strTableName);
				tableList.add(newTable);
				String path = "src/main/resources/data/" + strTableName;
				File file = new File(path);
				file.mkdir();
				file = new File(path + "/pages");
				file.mkdir();
				// Serializer.serialize(path + "/" + tableName + ".ser", table);
				serialize(newTable);

			}
			System.gc();
		} catch (IOException e) {

		}
	}

	public static void determineType(String str, String dataType)
			throws DBAppException {
		// Check if the string can be parsed as an integer
		switch (dataType) {
		case "java.lang.Integer":
			try {
				Integer.parseInt(str);
				break;
			} catch (Exception e) {
				throw new DBAppException(
						"MIN AND MAX SHOULD BE OF THE SAME DATA TYPE PLEASE!");
			}
		case "java.lang.Double":
			try {
				Double.parseDouble(str);
				break;
			} catch (Exception e) {
				throw new DBAppException(
						"MIN AND MAX SHOULD BE OF THE SAME DATA TYPE PLEASE!");
			}
		case "java.lang.String":
			try {
				//
				break;
			} catch (Exception e) {
				throw new DBAppException(
						"MIN AND MAX SHOULD BE OF THE SAME DATA TYPE PLEASE!");
			}
		case "java.util.Date":
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date date = dateFormat.parse(str);
				break;
			} catch (Exception e) {
				throw new DBAppException(
						"MIN AND MAX SHOULD BE OF THE SAME DATA TYPE PLEASE!");
			}
		}
	}

	public void writeToCSV(String input) {
		FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(csvFilePath, true);
			BufferedWriter bw = new BufferedWriter(fileWriter);
			bw.write(input);
			bw.newLine();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean checkTable(String name) {
		boolean isExists = false;
		try (BufferedReader reader = new BufferedReader(new FileReader(
				csvFilePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] columns = line.split(",");
				if (columns[0].trim().equals(name)) {
					isExists = true;
					break;
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isExists;
	}

	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// check if table exists
		try {
			boolean isExists = checkTable(strTableName);

			if (!isExists) {
				throw new DBAppException("Table not found");
			}
			// if exists deserialize --> insert --> serialize
			Table t;

			t = (Table) deserialize(strTableName);
			t.insertIntoTable(strTableName, htblColNameValue);
			serialize(t);
			t = null;

			System.gc();
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new DBAppException("Table not found");
		}
	}

	public static String checkType(Object key) throws ParseException {

		String type = key.getClass().toString();
		// System.out.println(primaryKey);

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

	public static Table deserialize(String tableName) throws IOException,
			ClassNotFoundException {
		String fileName = "src/main/resources/data/" + tableName + "/"
				+ tableName + ".ser";
		Table t = null;
		File file = new File(fileName);
		if (file.exists()) {
			FileInputStream fileStream = new FileInputStream(fileName);
			ObjectInputStream is = new ObjectInputStream(fileStream);
			t = (Table) is.readObject();
			fileStream.close();
			is.close();
			return t;
		}
		return t;
	}

	public static void serialize(Table table) throws IOException {
		// String fileName = "src/main/resources/" + table.tableName + ".ser";
		String fileName = "src/main/resources/data/" + table.tableName + "/"
				+ table.tableName + ".ser";
		FileOutputStream fileOut = new FileOutputStream(fileName);

		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);

		try {
			objOut.writeObject(table);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		objOut.close();
		fileOut.close();
	}

	public static int getconfig() {
		int noOfRows;

		Properties config = new Properties();
		try {
			InputStream input = new FileInputStream(new File(
					"src/main/resources/DBApp.config"));
			config.load(input);
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		noOfRows = Integer.parseInt(config
				.getProperty("MaximumRowsCountinPage"));

		return noOfRows;
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			boolean isExists = checkTable(strTableName);
			if (!isExists) {
				throw new DBAppException("Table not found");
			}
			// if exists deserialize --> insert --> serialize
			Table t;

			t = (Table) deserialize(strTableName);
			t.updateInTable(strTableName, strClusteringKeyValue,
					htblColNameValue);
			serialize(t);
			t = null;

			System.gc();
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new DBAppException();
		}
	}

	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			boolean isExists = false;
			BufferedReader reader = new BufferedReader(new FileReader(
					"src/main/resources/" + "metadata.csv"));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] columns = line.split(",");
				if (columns[0].trim().equals(strTableName)) {
					isExists = true;

					break;
				}
			}
			reader.close();

			if (!isExists) {
				throw new DBAppException("Table not found");
			}

			Table table = deserialize(strTableName);
			table.deleteFromTable(htblColNameValue);
			serialize(table);
			table = null;

			System.gc();

		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
			throw new DBAppException();
		}
	}
}
