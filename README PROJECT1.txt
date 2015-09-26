Team 6
Andrew Roskuski
Kevin Zhao

These files were either added or modified during this project and have been included in this submission. 
Buffer.java
BasicBufferMgr.java
BufferMgr.java
ClockReplacement.java
LeastRecentlyUsed.java
IBufferManager.java
NewBufferMgr.java
ReplacementPolicy.java
SampleQueries.java
Examples.sql
TestClock.java
TestLRU.java

All of these files should be imported into simpledb2.10. If using Eclipse, Buffer, BasicBufferMgr, BufferMgr,
ClockReplacement, LeastRecentlyUsed, NewBufferMgr, IBufferManager and ReplacementPolicy files should belong 
under simpledb.buffer. SampleQueries, TestClock and TestLRU should belong under the default package of 
studentyClient.simpledb. Examples should not belong in any folder and can exist in the uppermost level
alongside the README file. In order to run SampleQueries, TestClock, or TestLRU, a blank database should be
used and database directory will be studentdb for all three files.

Part 2.4
Both frame pinning and writing dirty frames to disk are both part of the Buffer.java file.

The "pins" variable denotes whether or not the buffer is pinned. If it is pinned, this value
is equal to 1. If not, it is equal to 0. Initially, this value starts at 0. The "pin" method 
increments the "pins" value by 1, setting the value to 1, therefore meaning it is pinned. The 
"unpin" method lowers the "pins" value by 1, setting it to 0 and denoting it unpinned. The 
isPinned method checks whether or not the "pins" value is greater than 1 and if it is, it
returns true.

"modifiedBy" is a value that denotes whether or not a buffer was modified by a transaction and
which transaction it was. If a positive value is there, then it means it was recently modified by
a transaction, the integer value denoting what transaction modified it. The isModifiedBy method
checks whether or not the buffer was modified during a certain transaction. If it was, then the
buffer is dirty and needs to be flushed. If the buffer is dirty, then the flush method is used to
write the page back to it's disk block and it's "modifiedBy" since the buffer was written to disk