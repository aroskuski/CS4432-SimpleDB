package simpledb.buffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 *
 */
class NewBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private Queue<Integer> free;
   private HashMap<Block, Buffer> blockIndex;
   private ClockReplacement CRU;
   private LeastRecentlyUsed LRU;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   NewBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      free = new LinkedList<Integer>();
      blockIndex = new HashMap<Block, Buffer>();
      
      for (int i=0; i<numbuffs; i++){
         bufferpool[i] = new Buffer();
         free.add(i);
      }
      
      CRU.fillArray(numbuffs);
      LRU.fillArray(numbuffs);
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         if(blockIndex.get(buff.block()) != null){
        	 blockIndex.remove(buff.block());
         }
         buff.assignToBlock(blk);
         blockIndex.put(blk, buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      if(blockIndex.get(buff.block()) != null){
     	 blockIndex.remove(buff.block());
      }
      buff.assignToNew(filename, fmtr);
      blockIndex.put(buff.block(), buff);
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      return blockIndex.get(blk);
   }

   private synchronized Buffer chooseUnpinnedBuffer() {

	   Integer buffIndex = free.poll();
	   if(buffIndex != null){
		   return bufferpool[buffIndex];
	   } else {
		   
		   for (Buffer buff : bufferpool){
			   if (!buff.isPinned()){
				   return buff;
			   }
		   }
		   return null;
	   }
	   
   }
   
   
}
