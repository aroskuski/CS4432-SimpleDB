package simpledb.index.hash;

import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/*CS4432 Describes each bucket in the extensible hash index*/
public class ExtensibleHashBucket {
	private static final int MAX_ENTRIES = 30;
	private TableInfo ti;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	public final String name;
	
	public ExtensibleHashBucket(String bucketName, Schema bucketSch, Transaction tx) {
		this.ti = new TableInfo(bucketName, bucketSch);
		this.tx = tx;
		this.name = bucketName;
	}
	
	/*CS4432 Sets the search key in the index and creates a new tablescan*/
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		ts = new TableScan(ti, tx);
	}
	
	/*CS4432 Gets the next item in the table*/
	public boolean next() {
		while (ts.next()){
			if (ts.getVal("dataval").equals(searchkey)){
				return true;
			}
		}
		
		return false;
	}
	
	/*CS4432 Gets the id of the record*/
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}
	
	/*CS4432 Adds an element from the extensible hash index*/
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}
	
	/*CS4432 Removes an element from the extensible hash index*/
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next()){
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
		}
	}
	
	/*CS4432 Closes the hash index*/
	public void close(){
		if (ts != null){
			ts.close();
		}
	}
	
	/*CS4432 Checks if the index is full*/
	public boolean isFull(){
		ts.beforeFirst();
		int tuples = 0;
		while (ts.next()){
			tuples++;
		}
		
		return tuples >= MAX_ENTRIES;
	}
	
	private TableScan getTableScan(){
		return ts;
	}
	
	/*CS4432 Copies information from a separate bucket*/
	public void copyfrom(ExtensibleHashBucket b, int hash, int bitmask){
		TableScan bts = b.getTableScan();
		ts = new TableScan(ti, tx);
		bts.beforeFirst();
		while(bts.next()){
			if((bts.getVal("dataval").hashCode() & bitmask) == hash){
				ts.insert();
				ts.setInt("block", bts.getInt("block"));
				ts.setInt("id", bts.getInt("id"));
				ts.setVal("dataval", bts.getVal("dataval"));
				bts.delete();
			}
		}
	}
}
