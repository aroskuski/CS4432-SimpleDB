package simpledb.buffer;

import simpledb.file.Block;

/*CS4432-Project1 The interface for the buffer manager. If this interface is implemented,
 * then all of the functions listed here must be used in the class.*/
public interface IBufferManager {

	/*CS4432-Project1 Flushes the buffer pool*/
	void flushAll(int txnum);
	
	/*CS4432-Project1 Pins a page*/
	Buffer pin(Block blk);
	
	/*CS4432-Project1 Adds a new page to the buffer pool and pins it*/
	Buffer pinNew(String filename, PageFormatter fmtr);
	
	/*CS4432-Project1 Unpins a page*/
	void unpin(Buffer buff);
	
	/*CS4432-Project1 Sees how many buffers are empty.*/
	int available();
	
	
}
