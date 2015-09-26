package simpledb.buffer;

import simpledb.file.Block;

public interface IBufferManager {

	void flushAll(int txnum);
	
	Buffer pin(Block blk);
	
	Buffer pinNew(String filename, PageFormatter fmtr);
	
	void unpin(Buffer buff);
	
	int available();
	
	
}
