package btree;

import chainexception.ChainException;

public class BTreeFileExistsException extends ChainException {
    public BTreeFileExistsException(Exception e, String message) {
        super(e, message);
    }
}
