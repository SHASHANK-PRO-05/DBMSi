package cmdline;

import bufmgr.BufMgr;
import columnar.ColumnarFile;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class ColumnarNestedLoopJoinTest {
    private final String columnarDBName = "test.col";
    private final String columnarOuterFileName = "People";
    private final String columnarInnerFileName = "Employee";
    private AttrType[] outerAttributes;
    private AttrType[] innerAttributes;

    @Before
    public void setupDatabase() throws Exception {
        SystemDefs systemDefs = new SystemDefs(columnarDBName, 1000, 200, "LRU");

        outerAttributes = new AttrType[3];
        outerAttributes[0] = new AttrType(AttrType.attrInteger);
        outerAttributes[0].setColumnId(0);
        outerAttributes[0].setSize((short) 4);
        outerAttributes[0].setAttrName("pid");
        outerAttributes[1] = new AttrType(AttrType.attrString);
        outerAttributes[1].setColumnId(1);
        outerAttributes[1].setSize((short) 12);
        outerAttributes[1].setAttrName("name");
        outerAttributes[2] = new AttrType(AttrType.attrInteger);
        outerAttributes[2].setColumnId(2);
        outerAttributes[2].setSize((short) 4);
        outerAttributes[2].setAttrName("age");

        ColumnarFile columnarOuterFile = new ColumnarFile(columnarOuterFileName, outerAttributes.length, outerAttributes);

        ArrayList<Person> people = new ArrayList<>();
        people.add(new Person(1, "Akshar", 30));
        people.add(new Person(2, "Shashank", 25));

        Tuple t = new Tuple();
        t.setHdr((short) 3, outerAttributes, new short[]{12});

        t = new Tuple(t.size());
        t.setHdr((short) 3, outerAttributes, new short[]{12});

        for (Person person : people) {
            t.setIntFld(1, person.pid);
            t.setStrFld(2, person.name);
            t.setIntFld(3, person.age);
            columnarOuterFile.insertTuple(t.getTupleByteArray());
        }

        innerAttributes = new AttrType[2];
        innerAttributes[0] = new AttrType(AttrType.attrInteger);
        innerAttributes[0].setColumnId(0);
        innerAttributes[0].setSize((short) 4);
        innerAttributes[0].setAttrName("eid");
        innerAttributes[1] = new AttrType(AttrType.attrString);
        innerAttributes[1].setColumnId(1);
        innerAttributes[1].setSize((short) 12);
        innerAttributes[1].setAttrName("department");

        ColumnarFile columnarInnerFile = new ColumnarFile(columnarInnerFileName, innerAttributes.length, innerAttributes);

        ArrayList<Employee> employees = new ArrayList<>();
        employees.add(new Employee(1, "CS"));

        Tuple p = new Tuple();
        p.setHdr((short) 2, innerAttributes, new short[]{12});

        p = new Tuple(p.size());
        p.setHdr((short) 2, innerAttributes, new short[]{12});

        for (Employee employee : employees) {
            p.setIntFld(1, employee.eid);
            p.setStrFld(2, employee.department);
            columnarInnerFile.insertTuple(p.getTupleByteArray());
        }

        SystemDefs.JavabaseBM.flushAllPages();
    }

    @Test
    public void testNestedLoopJoin() throws Exception {
        String programArgumentsString = String.format("%s %s %s [ pid == 1 ] [ eid == 1 ] [ pid == eid ] None [ pid name ] 100",
            columnarDBName, columnarOuterFileName, columnarInnerFileName);
        String[] programArguments = programArgumentsString.split(" ");

        ColumnarNestedLoopJoin.main(programArguments);

        System.out.println(Arrays.toString(programArguments));
    }

    @After
    public void deleteDatabase() {
        File file = new File(columnarDBName);

        boolean success = file.delete();

        System.out.println(success);
    }
}

class Person {
    public int pid;
    public String name;
    public int age;

    public Person(int pid, String name, int age) {
        this.pid = pid;
        this.name = name;
        this.age = age;
    }
}

class Employee {
    public int eid;
    public String department;

    public Employee(int eid, String department) {
        this.eid = eid;
        this.department = department;
    }
}