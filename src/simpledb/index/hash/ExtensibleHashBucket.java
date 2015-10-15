package simpledb.index.hash;

import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

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
	
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		ts = new TableScan(ti, tx);
	}
	
	public boolean next() {
		while (ts.next()){
			if (ts.getVal("dataval").equals(searchkey)){
				return true;
			}
		}
		
		return false;
	}
	
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}
	
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}
	
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next()){
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
		}
	}
	
	public void close(){
		if (ts != null){
			ts.close();
		}
	}
	
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
	
	public void copyfrom(ExtensibleHashBucket b, int hash, int bitmask){
		TableScan bts = b.getTableScan();
		ts = new TableScan(ti, tx);
		bts.beforeFirst();
		while(bts.next()){
			if((bts.getVal("dataVal").hashCode() & bitmask) == hash){
				ts.insert();
				ts.setInt("block", bts.getInt("block"));
				ts.setInt("id", bts.getInt("id"));
				ts.setVal("dataval", bts.getVal("dataval"));
				bts.delete();
			}
		}
	}
}
