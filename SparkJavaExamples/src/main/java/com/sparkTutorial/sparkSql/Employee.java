package com.sparkTutorial.sparkSql;

import java.io.Serializable;

/**
 * Created by chas6003 on 26-04-2019.
 */
public class Employee implements Serializable {
    private int eid;
    private String ename;
    private int dno;

    public Employee(int eid, String ename, int dno) {
        this.eid = eid;
        this.ename = ename;
        this.dno = dno;
    }

    public Employee() {}

    public void setEid(int eid) {
        this.eid = eid;
    }

    public int getEid() {
        return this.eid;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public String getEname() {
        return this.ename;
    }

    public void setDno(int dno) {
        this.dno = dno;
    }

    public int getDno() {
        return this.dno;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "eid=" + eid +
                ", ename='" + ename + '\'' +
                ", dno=" + dno +
                '}';
    }
}
