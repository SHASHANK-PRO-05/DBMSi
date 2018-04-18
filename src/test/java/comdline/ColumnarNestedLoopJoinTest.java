package comdline;

import columnar.ColumnarFile;
import global.AttrType;
import global.SystemDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class ColumnarNestedLoopJoinTest {
    private final String columnarDBName = "test.col";
    private final String columnarOuterFileName = "People";
    private final String columnarInnerFileName = "Employee";
    private AttrType[] outerAttributes;
    private AttrType[] innerAttributes;

    @Before
    public void setupDatabase() throws Exception {
        SystemDefs systemDefs = new SystemDefs(columnarDBName, 10000, 10000, "LRU");

        outerAttributes = new AttrType[3];
        outerAttributes[0] = new AttrType(AttrType.attrInteger);
        outerAttributes[1] = new AttrType(AttrType.attrString);
        outerAttributes[2] = new AttrType(AttrType.attrInteger);

        ColumnarFile columnarOuterFile = new ColumnarFile(columnarOuterFileName, outerAttributes.length, outerAttributes);
    }

    @Test
    public void testNestedLoopJoin() {
        assert true;
    }

    @After
    public void deleteDatabase() {
        File file = new File(columnarDBName);

        boolean success = file.delete();

        System.out.println(success);
    }
}

class People {
    private int pid;
    private String name;
    private int age;

    public People(int pid, String name, int age) {
        this.pid = pid;
        this.name = name;
        this.age = age;
    }
}

class Employee {
    private int eid;
    private String department;

    public Employee(int eid, String department) {
        this.eid = eid;
        this.department = department;
    }
}