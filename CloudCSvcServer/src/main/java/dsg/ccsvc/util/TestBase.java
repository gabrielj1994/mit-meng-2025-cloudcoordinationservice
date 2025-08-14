package dsg.ccsvc.util;

import org.apache.commons.lang3.NotImplementedException;

public abstract class TestBase implements TestInterface{

    public String interfaceMethodTest() {
        throw new NotImplementedException();
    }


    public void close() {
        throw new NotImplementedException();
    }
}
