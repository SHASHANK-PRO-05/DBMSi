package bitmap;

import chainexception.ChainException;

public class BitMapFileCreationException extends ChainException {
    BitMapFileCreationException(Exception e, String s) {
        super(e, s);
    }
}
