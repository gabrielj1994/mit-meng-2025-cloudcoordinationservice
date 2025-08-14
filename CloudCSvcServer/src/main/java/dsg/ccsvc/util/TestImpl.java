package dsg.ccsvc.util;

public class TestImpl extends TestBase{

    public String interfaceMethodTest() {
        return "TestImpl";
    }

    public void close() {
        System.out.println("TestImpl");
    }
}
