package net.nzcorp.hbase.exceptions;

import org.apache.hadoop.hbase.coprocessor.CoprocessorException;

public class MissingIndexKeyException extends CoprocessorException {

    public MissingIndexKeyException() {
        super();
    }

    public MissingIndexKeyException(Class cls, String msg) {
        super(cls, msg);
    }

    public MissingIndexKeyException(String msg) {
        super(msg);
    }
}
