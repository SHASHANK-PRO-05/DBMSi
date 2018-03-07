package columnar;

import chainexception.ChainException;

public class ColumnClassCreationException extends ChainException {
    public ColumnClassCreationException(Exception e, String name) {
        super(e, name);
    }
}
