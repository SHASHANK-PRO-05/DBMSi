package iterator;

import chainexception.ChainException;

public class ColumnarFileScanException extends ChainException {
    public ColumnarFileScanException(Exception e, String string) {
        super(e, string);
    }
}
