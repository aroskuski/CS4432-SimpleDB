package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.buffer.PageFormatter;
import simpledb.index.Index;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Edward Sciore
 */
public class ExtensibleHashIndex implements Index {
	public static int NUM_BUCKETS = 100;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	private TableInfo bucketTi, indTi;
	private int precision;
	private int bucketPrecision;
	private ExtensibleHashBucket bucket = null;

	/**
	 * Opens a hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	public ExtensibleHashIndex(String idxname, Schema bucketSch, Transaction tx) {
		this.idxname = idxname;
		this.sch = bucketSch;
		
		//deal with the 2nd level index
		Schema indexsch = new Schema();
		indexsch.addIntField("hash");
		indexsch.addIntField("precision");
		indexsch.addStringField("bucket", 50);
		String indextbl = idxname + "ind";
		indTi = new TableInfo(indextbl, indexsch);
		if(tx.size(indTi.fileName()) == 0){// If empty, initialize values
			this.precision = 1;
			//create index entries
			TableScan ts = new TableScan(indTi, tx);
			ts.insert();
			ts.setInt("hash", 1);
			ts.setInt("precision", 1);
			ts.setString("bucket", "1");
			ts.insert();
			ts.setInt("hash", 0);
			ts.setInt("precision", 1);
			ts.setString("bucket", "0");
			ts.close();
			
		} else {
			TableScan ts = new TableScan(indTi, tx);
			ts.next();
			this.precision = ts.getInt("precision");
			ts.close();
		}
		
		/*
		// deal with the buckets
		String buckettbl = idxname + "leaf";
		bucketTi = new TableInfo(buckettbl, bucketSch);
		if (tx.size(bucketTi.fileName()) == 0){
			tx.append(bucketTi.fileName(), new EHPageFormatter(bucketTi, -1, 1));
			tx.append(bucketTi.fileName(), new EHPageFormatter(bucketTi, -1, 1));
		}
		*/


		
		this.tx = tx;
		
	}

	/**
	 * Positions the index before the first index record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		TableScan ts = new TableScan(indTi, tx);
		int hash = searchkey.hashCode() & genBitmask();
		String bucketName = idxname;
		while(ts.next()){
			if(ts.getInt("hash") == hash){
				bucketName += ts.getString("bucket");
				
				break;
			}
		}
		
		bucketPrecision = ts.getString("bucket").length();
		
		ts.close();
		
		bucket = new ExtensibleHashBucket(bucketName, sch, tx);
		
		bucket.beforeFirst(searchkey);
	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		return bucket.next();
	}

	/**
	 * Retrieves the dataRID from the current record
	 * in the table scan for the bucket.
	 * @see simpledb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		return bucket.getDataRid();
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		if(precision < 32){
			if(bucket.isFull()){
				split();
			}
			bucket.beforeFirst(val);
		}
		bucket.insert(val, rid);
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		bucket.delete(val, rid);
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	public void close() {
		if (bucket != null){
			bucket.close();
		}
		bucket = null;
	}

	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / ExtensibleHashIndex.NUM_BUCKETS;
	}
	
	private int genBitmask(){
		return (~0x0) >> (32 - precision);
	}
	
	private void split(){
		
	}
}
