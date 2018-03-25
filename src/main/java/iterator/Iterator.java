package iterator;

import bufmgr.*;
import global.Flags;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

/**
 * All the relational operators and access methods are iterators.
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
     * abstract method, every subclass must implement it.
     *
     * @return the result tuple
     * @throws IOException               I/O errors
     * @throws IndexException            exception from super class
     * @throws InvalidTupleSizeException invalid tuple size
     * @throws InvalidTypeException      tuple type not valid
     * @throws PageNotReadException      exception from lower layer
     * @throws TupleUtilsException       exception from using tuple utilities
     * @throws UnknownKeyTypeException   key type unknown
     * @throws Exception                 other exceptions
     */
    public abstract Tuple getNext()
            throws IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            Exception;

    public abstract int getNextPosition() throws Exception;

    /**
     * @throws IOException                 I/O errors
     * @throws IndexException              exception from Index class
     * @throws ReplacerException
     * @throws HashEntryNotFoundException
     * @throws InvalidFrameNumberException
     * @throws PageUnpinnedException
     */
    public abstract void close()
            throws IOException, IndexException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException;


}
