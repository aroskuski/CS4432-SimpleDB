package simpledb.buffer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import simpledb.file.*;

/**
 * CS4432-Project1
 * This BufferManager is a brand new buffer manager that is based on
 * BasicBufferMgr.java
 * Manages the pinning and unpinning of buffers to blocks.
 *
 */
class NewBufferMgr implements IBufferManager{
   private Buffer[] bufferpool;
   private int numAvailable;
   /*CS4432-Project1*/
   private Queue<Integer> free;
   private HashMap<Block, Buffer> blockIndex;
   /*CS4432-Project1*/
   private ReplacementPolicy rp;
   
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
   NewBufferMgr(int numbuffs, boolean clock) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      free = new LinkedList<Integer>();
      /*CS4432-Project1*/
      blockIndex = new HashMap<Block, Buffer>();
      
      for (int i=0; i<numbuffs; i++){
    	  /*CS4432-Project1 creates a new buffer and makes sure it is
    	   * given an id value*/
         bufferpool[i] = new Buffer(i);
         free.add(i);
      }
      
      /*CS4432-Project1 Initializes the replacement policies by
       * setting the bufferpool references and filling the array
       * indexes with neutral numbers.*/
      if (clock){
    	  rp = new ClockReplacement(bufferpool);
      } else {
    	  rp = new LeastRecentlyUsed(bufferpool);
      }
      
      //LRU = new LeastRecentlyUsed(bufferpool);
      rp.fillArray(numbuffs);
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   public synchronized void flushAll(int txnum) {
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
   public synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         /*CS4432-Project1*/
         if(blockIndex.get(buff.block()) != null){
        	 blockIndex.remove(buff.block());
         }
         buff.assignToBlock(blk);
         /*CS4432-Project1*/
         blockIndex.put(blk, buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      
      /*CS4432-Project1 Let's the index array in the replacement
       * policies know that this buffer is now pinned. For LRU,
       * this means that this is the most recently used buffer,
       * for clock it means that this buffer is pinned.*/
      rp.pin(buff);
      
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
   public synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      /*CS4432-Project1*/
      if(blockIndex.get(buff.block()) != null){
     	 blockIndex.remove(buff.block());
      }
      buff.assignToNew(filename, fmtr);
      /*CS4432-Project1*/
      blockIndex.put(buff.block(), buff);
      numAvailable--;
      buff.pin();
      
      /*CS4432-Project1 Lets the replacement policies know that a buffer
       * has been added and that it is now pinned. For LRU, it means this
       * that this buffer pinned, and for Clock, move the hand to the 
       * next buffer.*/
      rp.newPin(buff);
      
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
      
      /*CS4432-Project1 Let's the replacement policies know that something was unpinned.
       * In the case of LRU, it means that this is the most recently used buffer and
       * for clock, it just means that the buffer is unpinned*/
      rp.unpin(buff);
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   public int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
	   /*CS4432-Project1*/
      return blockIndex.get(blk);
   }

   private synchronized Buffer chooseUnpinnedBuffer() {

	   /*CS4432-Project1*/
	   Integer buffIndex = free.poll();
	   if(buffIndex != null){
		   return bufferpool[buffIndex];
	   } else {
		   return rp.indexToReplace();

	   }
	   
   }
   
   /*CS4432-Project1 The new toString method of the buffer manager. Uses
    * the Buffer's newToString method to get the info.*/
   @Override
   public String toString(){
	 String bufferInfo = "Replacement policy: " + rp.getClass().getSimpleName() + '\n';
	 bufferInfo += Arrays.toString(bufferpool);
	 return bufferInfo;
   }
   
}
