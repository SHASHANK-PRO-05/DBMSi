package iterator;
import global.*;
import heap.*;
import diskmgr.*;
import bufmgr.*;
import java.io.*;

/**
 *All the relational operators and access methods are iterators.
 */
public abstract class Iterator implements Flags {
  
  /**
   * a flag to indicate whether this iterator has been closed.
   * it is set to true the first time the <code>close()</code> 
   * function is called.
   * multiple calls to the <code>close()</code> function will
   * not be a problem.
   */
  public boolean closeFlag = false; // added by bingjie 5/4/98

  /**
   *abstract method, every subclass must implement it.
   *@return the result tuple
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class    
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception TupleUtilsException exception from using tuple utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions
   */
  public abstract Tuple getNext() 
    throws IOException,
	   InvalidTupleSizeException,
	   InvalidTypeException, 
	   PageNotReadException,
	   TupleUtilsException, 
	   Exception;

  /**
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from Index class
 * @throws ReplacerException 
 * @throws HashEntryNotFoundException 
 * @throws InvalidFrameNumberException 
 * @throws PageUnpinnedException 
   *@exception SortException exception Sort class
   */
  public abstract void close() 
    throws IOException, IndexException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException; 
	 
  
  

  
  
}
