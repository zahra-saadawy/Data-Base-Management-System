package main.java;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class Page implements Serializable {
	Tuples min;
	Tuples max;
	int maxRows;
	transient Vector<Tuples> rows;
	int currentSize = 0;
	int pageNumber;
	Table parentTable;
	String clusteringKey;

	public Page(int maxRows, Table parentTable, int pageNumber) throws IOException {
		this.maxRows = maxRows;
		rows = new Vector<Tuples>(maxRows);
		this.parentTable = parentTable;
		this.pageNumber = pageNumber;
		clusteringKey = Table.clusteringKeyValue(parentTable.tableName);

	}// lazm ytshalll

	public Page(int maxRows2, String string, int pageNumber2) throws IOException {
		this.maxRows = maxRows2;
		rows = new Vector<Tuples>();
		this.parentTable = parentTable;
		this.pageNumber = pageNumber2;
		clusteringKey = Table.clusteringKeyValue(string);
	}// lazm ytshalll

	public void printRecords() {
		for (Tuples row : this.rows) {
			System.out.println(row.tuple.values());
		}
	}

	public void insertNewRecord(Hashtable<String, Object> htblColNameValue)
			throws ParseException, ClassNotFoundException, IOException, DBAppException {

		// Table.deserializable(parentTable.tableName,this.pageNumber);
		Tuples newTuple = new Tuples(htblColNameValue, clusteringKey);
		int index = findPlace(newTuple);
		// insert into index
		Vector<OctTree> allTrees = parentTable.trees;
		for (OctTree i : allTrees) {
			OctTree currTree = i;
			Object xIndex = htblColNameValue.get(currTree.xname);
			Object yIndex = htblColNameValue.get(currTree.yname);
			Object zIndex = htblColNameValue.get(currTree.zname);
			if (xIndex == null || yIndex == null || zIndex == null)
				continue;
			else {
				
				i.octTreeIndex.insert(xIndex, yIndex, zIndex,  this.pageNumber+" "+htblColNameValue.get(clusteringKey));
//				if (i.octTreeIndex.root) {
//					i.octTreeIndex.insert(xIndex, yIndex, zIndex, this.pageNumber);
//				} else {
//					i.octTreeIndex.remove(xIndex, yIndex, zIndex);
//					i.octTreeIndex.insert(xIndex, yIndex, zIndex, this.pageNumber);
//				}
			}

		}
		// -------------------------------------------------------------------------------------
		shift(index, newTuple);

		Table.serializeObject(this.rows, parentTable.tableName, this.pageNumber);
		this.rows = null;
	}

	public void updateExistingRecord(Hashtable<String, Object> htblColNameValue, String clusteringKey)
			throws ParseException, DBAppException, IOException {
		// finding the tuple
		Tuples tempTuple = new Tuples(htblColNameValue, clusteringKey);
		int updateTupleIndex = findTupletoUpdate(tempTuple);
		updateColumns(updateTupleIndex, htblColNameValue);
		Table.serializeObject(this.rows, parentTable.tableName, this.pageNumber);
		this.rows = null;

	}

	private void updateColumns(int updateTupleIndex, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
		Tuples updatedTuple = this.rows.get(updateTupleIndex);
		Vector<OctTree> allTrees = parentTable.trees;
		for (OctTree i : allTrees) {
			OctTree currTree = i;
			Object xIndex = updatedTuple.tuple.get(currTree.xname);
			Object yIndex = updatedTuple.tuple.get(currTree.yname);
			Object zIndex = updatedTuple.tuple.get(currTree.zname);
			if (xIndex == null || yIndex == null || zIndex == null)
				continue;
			else {
				i.octTreeIndex.remove(new OctPoint(xIndex, yIndex, zIndex,this.pageNumber),i.octTreeIndex.root);
			}

		}
		Hashtable<String, Object> tupleData = updatedTuple.tuple;
		for (String Key : htblColNameValue.keySet()) {
			ArrayList data = parentTable.readCsvCKDataType(this.parentTable.tableName, Key);
			if (data == null || data.size() == 0)
				throw new DBAppException("No Column with the specified Name to update");
			else {
				tupleData.put(Key, htblColNameValue.get(Key));
			}

		}
		for (OctTree i : allTrees) {
			OctTree currTree = i;
			Object xIndex = tupleData.get(currTree.xname);
			Object yIndex = tupleData.get(currTree.yname);
			Object zIndex = tupleData.get(currTree.zname);
			if (xIndex == null || yIndex == null || zIndex == null)
				continue;
			else {
				i.octTreeIndex.insert(xIndex, yIndex, zIndex, this.pageNumber);
			}

		}

	}

	private int findTupletoUpdate(Tuples tempTuple) throws DBAppException {
		int left = 0;
		int right = this.rows.size() - 1;

		while (left <= right) {
			int mid = (left + right) / 2;

			if (mid > this.rows.size() - 1)
				break;
			else if (this.rows.get(mid).compareTo(tempTuple) > 0) {
				right = mid - 1;
			} else if (this.rows.get(mid).compareTo(tempTuple) < 0)
				left = mid + 1;
			else if (this.rows.get(mid).compareTo(tempTuple) == 0)
				return mid;

		}
		throw new DBAppException("No tuple found with this ClusteringKey Value");
	}

	public int findPlace(Tuples newTuple) {
		// if first one to insert in the page

		if (this.rows.size() == 0 || this.min == null && this.max == null)
			return 0;
		int left = 0;
		int right = this.rows.size() - 1;

		if (newTuple.compareTo(this.min) < 0) {
			return 0;
		} else if (newTuple.compareTo(this.max) > 0) {
			return this.rows.size();
		}
		while (left <= right) {
			int mid = (left + right) / 2;
			int midplusone = mid + 1;

			if (midplusone > this.rows.size() - 1)
				break;
			if (this.rows.get(mid).compareTo(newTuple) < 0 && (this.rows.get(midplusone).compareTo(newTuple) > 0)) {

				return midplusone;
			} else if (this.rows.get(mid).compareTo(newTuple) > 0) {
				right = mid - 1;
			} else if (this.rows.get(midplusone).compareTo(newTuple) < 0)
				left = mid + 1;

		}
		return -1;

	}

	public void shift(int index, Tuples htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException, ParseException {
		Hashtable<String, Object> temp;

		this.rows.add(index, htblColNameValue);
		if (rows.size() > maxRows) {
			if (parentTable.tablePages.size() == this.pageNumber)

			{
				Page p = new Page(this.maxRows, this.parentTable, this.pageNumber + 1);
				parentTable.tablePages.add(p);
				Table.serializeObject(p.rows, this.parentTable.tableName, this.pageNumber + 1);
				p.rows = null;
			}
			this.parentTable.tablePages.get(this.pageNumber).rows = Table.deserializable(this.parentTable.tableName,
					this.pageNumber + 1);

			// inserting into indexes
			Hashtable<String, Object> tempTuple = rows.get(maxRows).tuple;
			Vector<OctTree> allTrees = parentTable.trees;

			for (OctTree i : allTrees) {
				OctTree currTree = i;
				Object xIndex = tempTuple.get(currTree.xname);
				Object yIndex = tempTuple.get(currTree.yname);
				Object zIndex = tempTuple.get(currTree.zname);
//				if (i.octTreeIndex.root) {
//					i.octTreeIndex.insert(xIndex, yIndex, zIndex, this.pageNumber);
//				} else {
//					i.octTreeIndex.remove(xIndex, yIndex, zIndex);
//					i.octTreeIndex.insert(xIndex, yIndex, zIndex, this.pageNumber);
//				}
				i.octTreeIndex.remove(new OctPoint(xIndex, yIndex, zIndex,this.pageNumber+" "+tempTuple.get(clusteringKey)),i.octTreeIndex.root);
				

			}

			// --------------------------------------------------------------------------------
			parentTable.tablePages.get(this.pageNumber).insertNewRecord(rows.remove(maxRows).tuple);
		}

		min = this.rows.get(0);// to re adjust min and max
		max = this.rows.get(rows.size() - 1);

	}

	// verify if there is no clustering key has the same value
	public boolean isExist(Tuples value) {

		int low = 0;
		int high = rows.size() - 1;

		while (low <= high) {
			int mid = (low + high) / 2;
			Tuples midValue = rows.get(mid);

			int cmp = midValue.compareTo(value);

			if (cmp < 0) {
				low = mid + 1;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				return true;
			}
		}

		return false;
	}

	public void deleteRecordWithClusteringKey(Hashtable<String, Object> htblColNameValue)
			throws ParseException, DBAppException, IOException {
		Vector<OctTree> allTrees = parentTable.trees;
		Tuples row = new Tuples(htblColNameValue, clusteringKey);
		int index = findTupletoUpdate(row);
		if (index != -1) {

			Hashtable<String, Object> pageContent = this.rows.get(index).tuple;
			boolean delete = true;
			for (String key : htblColNameValue.keySet()) {
				if (!pageContent.containsKey(key)) {
					throw new DBAppException("No Column with the specified Name to delete");
				} else {
					if (!htblColNameValue.get(key).equals(pageContent.get(key))) {
						delete = false;
						break;
					}
				}
			}
			if (delete) {
				for (OctTree i : allTrees) {
					OctTree currTree = i;
					Object xIndex = rows.get(index).tuple.get(currTree.xname);
					Object yIndex = rows.get(index).tuple.get(currTree.yname);
					Object zIndex = rows.get(index).tuple.get(currTree.zname);
					if (xIndex == null || yIndex == null || zIndex == null)
						continue;
					else {
						i.octTreeIndex.remove(new OctPoint(xIndex, yIndex, zIndex,this.pageNumber+" "+htblColNameValue.get(clusteringKey)),i.octTreeIndex.root);
					}

				}
				rows.remove(index);
				
			} else {
				throw new DBAppException("Record does not exist!");
			}
		} else {
			throw new DBAppException("Record does not exist!");
		}
		if (!rows.isEmpty()) {
			min = rows.get(0);
			max = rows.get(rows.size() - 1);
		}
		Table.serializeObject(this.rows, this.parentTable.tableName, this.pageNumber);
		this.rows = null;
	}

	public void deleteRecord(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		Iterator<Tuples> iterator = this.rows.iterator();
		Vector<OctTree> allTrees = parentTable.trees;
		while (iterator.hasNext()) {
			Tuples row = iterator.next();
			Hashtable<String, Object> pageContent = row.tuple;
			boolean delete = true;
			for (String key : htblColNameValue.keySet()) {
				if (!pageContent.containsKey(key) || !htblColNameValue.get(key).equals(pageContent.get(key))) {
					delete = false;
					break;
				}
			}
			if (delete) {
				for (OctTree i : allTrees) {
					OctTree currTree = i;
					Object xIndex = row.tuple.get(currTree.xname);
					Object yIndex = row.tuple.get(currTree.yname);
					Object zIndex = row.tuple.get(currTree.zname);
					if (xIndex == null || yIndex == null || zIndex == null)
						continue;
					else {
						i.octTreeIndex.remove(new OctPoint(xIndex, yIndex, zIndex,this.pageNumber+" "+row.tuple.get(clusteringKey)),i.octTreeIndex.root);
					}

				}
				iterator.remove();
			
			}
		}
		if (!rows.isEmpty()) {
			min = rows.get(0);
			max = rows.get(rows.size() - 1);
		}
		Table.serializeObject(this.rows, this.parentTable.tableName, this.pageNumber);
		this.rows = null;
	}

	// public void update (Object clusteringKeyValue,Hashtable<String,Object>
	// updatedRow){
	// Tuples update= new Tuples(updatedRow,clusteringKey);
	// int left=0;
	// int right=this.rows.size()-1;
	// while(left<=right)
	// { int mid=left+right/2;
	// if ( this.rows.get(mid).compareTo(clusteringKeyValue)==0)
	// rows.set(mid,updatedRow);
	// else if ( this.rows.get(mid).compareTo(clusteringKeyValue)>0)
	// right=mid-1;
	// else
	// left=mid+1;
	//
	// }
	// }

	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, DBAppException {
		// Page a = new Page(6, "d", 1);
		// Hashtable<String, Object> row1 = new Hashtable();
		// row1.put("id", new Integer(7));
		// row1.put("name", new String("Zaky Noor"));
		// row1.put("gpa", new Double(0.88));
		//
		// Tuples t1 = new Tuples(row1, "id");
		// a.rows.add(0, t1);
		// a.min = t1;
		// a.max = t1;

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
		// Hashtable<String, Object> row4 = new Hashtable();
		// row4.put("id", new Integer( 8));
		// row4.put("name", new String("gego badrawy" ) );
		// row4.put("gpa", new Double( 1.7 ) );
		//
		//
		// Tuples t4= new Tuples(row4,"id");
		// // System.out.print(a.findPlace(t4));
		//
		// a.shift(1, t4);
		// Hashtable<String, Object> row5 = new Hashtable();
		// row5.put("id", new Integer( 0));
		// row5.put("name", new String("gego badrawy" ) );
		// row5.put("gpa", new Double( 1.7 ) );
		//
		//
		// Tuples t5= new Tuples(row5,"id");
		// a.shift(0, t5);
		//
		//
		// Hashtable<String, Object> row6 = new Hashtable();
		// row6.put("id", new Integer( 20));
		// row6.put("name", new String("gego badrawy" ) );
		// row6.put("gpa", new Double( 1.7 ) );
		//
		//
		// Tuples t6= new Tuples(row6,"id");
		// a.shift(5, t6);
		// Hashtable<String, Object> row7 = new Hashtable();
		// row7.put("id", new Integer( 22));
		// row7.put("name", new String("gego badrawy" ) );
		// row7.put("gpa", new Double( 1.7 ) );
		//
		//
		// // Tuples t7= new Tuples(row7,"id");
		// // a.shift(6, t7);
		// Hashtable<String, Object> row8 = new Hashtable();
		// row8.put("id", new Integer(23));
		// row8.put("name", new String("gego badrawy"));
		// row8.put("gpa", new Double(1.7));
		// // Tuples t8= new Tuples(row8,"id");
		// // // a.shift(7, t8);
		//
		// // Table.serializeObject(a, "d");
		// Table.deserializable("d", 1);
		// a.insertNewRecord(row8);
		//
		// for (int i = 0; i < a.rows.size(); i++)
		// System.out.println(i + ""
		// + a.rows.get(i).tuple.get(a.clusteringKey));
		// Vector<Tuples> a=Table.deserializable("Yoh", 1);
		// System.out.print(a.get(1).tuple);
		// Table.serializeObject(a, "yoh", 1);

	}

}
