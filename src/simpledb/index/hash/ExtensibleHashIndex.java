package simpledb.index.hash;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.ArrayList;
import java.util.List;

import simpledb.index.Index;

/*CS4432 The code for the operations of the extensible hash
 * Index. Code is based on the HashIndex.java file*/

/**
 * A extensible hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 */
public class ExtensibleHashIndex implements Index {
	public static int NUM_BUCKETS = 100;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableInfo indTi;
	
	/*CS4432 The precision of the index */
	private int precision;
	private int bucketPrecision;
	
	/*CS4432 Information for the buckets of the extensible hash*/
	private ExtensibleHashBucket bucket = null;
	private String bucketPostfix = null;

	/**
	 * Opens a hash index for the specified index.
	 * @param idxname the name of the index
	 * @param bucketSch the schema of the index records
	 * @param tx the calling transaction
	 */
	public ExtensibleHashIndex(String idxname, Schema bucketSch, Transaction tx) {
		this.idxname = idxname;
		this.sch = bucketSch;
		
		/*Deals with the 2nd level index*/
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
				bucketName = bucketName + ts.getString("bucket");
				
				break;
			}
		}
		
		bucketPrecision = ts.getString("bucket").length();
		bucketPostfix = ts.getString("bucket");
		
		ts.close();
		
		bucket = new ExtensibleHashBucket(bucketName, sch, tx);
		
		bucket.beforeFirst(searchkey);
	}

	/* CS4432 Calls the bucket's next() function */
	public boolean next() {
		return bucket.next();
	}

	/* CS4432 Calls the bucket's getDataRid() function */
	public RID getDataRid() {
		return bucket.getDataRid();
	}

	/* CS4432 Inserts a new record into the table scan for the bucket
	 * Depending on the size of the bucket, it may not be able to hold
	 * the information so a split may be needed
	 */
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		if(bucketPrecision < 32){
			while(bucket.isFull() && bucketPrecision < 32){
				split();
				beforeFirst(val);
			}
			beforeFirst(val);
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
	
	/*CS4432 Generates the bitmask for the current precision*/
	private int genBitmask(){
		return genBitmask(precision);
	}
	
	/*CS4432 uses bitwise operations to get the precision
	 * of the bucket make sure elements are sorted correctly*/
	private int genBitmask(int precision){
		return (~0x0) >>> (32 - precision);
	}
	
	/*CS4432 Used when there are too many elements in a single
	 * index, we have to split the bucket. */
	private void split(){
		if(bucketPrecision == precision){
			splitIndex();
		}
		
		TableScan ts = new TableScan(indTi, tx);
		
		String bucketName = bucket.name;
		bucketName = bucketPostfix;
		int bucketHash = Integer.parseInt(bucketName, 2);
		int bucketHash1 = bucketHash | (1 << bucketPrecision);
		
		while(ts.next()){
			int currentHash = ts.getInt("hash");
			
			if((currentHash & genBitmask(bucketPrecision + 1)) == bucketHash){
				String newName = genBucketName(bucketHash, bucketPrecision + 1);
				ts.setString("bucket", newName);
				ExtensibleHashBucket b = new ExtensibleHashBucket(newName, sch, tx);
				b.copyfrom(bucket, bucketHash, genBitmask(bucketPrecision + 1));
			}
			
			if((currentHash & genBitmask(bucketPrecision + 1)) == bucketHash1){
				String newName = genBucketName(bucketHash1, bucketPrecision + 1);
				ts.setString("bucket", newName);
				ExtensibleHashBucket b = new ExtensibleHashBucket(newName, sch, tx);
				b.copyfrom(bucket, bucketHash1, genBitmask(bucketPrecision + 1));
			}

		}
		
		ts.close();
		
	}
	
	/*CS4432 If a bucket has the same precision as the whole index,
	 * then we need to increase the index precision and have that
	 * be reflected in the rest of the buckets*/
	private void splitIndex(){
		TableScan ts = new TableScan(indTi, tx);
		List<ExtensibleHashIndexTuple> hashes = new ArrayList<ExtensibleHashIndexTuple>();
		while(ts.next()){
			hashes.add(new ExtensibleHashIndexTuple(ts.getInt("hash"), 
					ts.getString("bucket")));
		}
		
		ts.beforeFirst();
		for (ExtensibleHashIndexTuple hash : hashes){
			ts.insert();
			ts.setInt("hash", hash.hash);
			ts.setInt("precision", precision + 1);
			ts.setString("bucket", hash.bucket);
			ts.insert();
			ts.setInt("hash", hash.hash | (1 << precision));
			ts.setInt("precision", precision + 1);
			ts.setString("bucket", hash.bucket);
		}
		
		ts.beforeFirst();
		
		while(next()){
			if(ts.getInt("precision") == precision){
				ts.delete();
			}
		}
		
		precision++;
		
	}
	/*CS4432 Creates the name of the bucket*/
	private String genBucketName(int hash, int precision){
		String result = Integer.toString(hash, 2);
		while(result.length() < precision){
			result = "0" + result;
		}
		return result;
	}
}
