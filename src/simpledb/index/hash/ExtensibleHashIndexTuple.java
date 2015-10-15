package simpledb.index.hash;

public class ExtensibleHashIndexTuple {
	public final int hash;
	public final String bucket;
	
	ExtensibleHashIndexTuple(int hash, String bucket){
		this.hash = hash;
		this.bucket = bucket;
	}

}
