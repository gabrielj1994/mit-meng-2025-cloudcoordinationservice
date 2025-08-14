package org.example.util;

import org.example.model.Record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PSQLUtil {

    final static String TABLE_NAME = "zktest_records";
    final static String CONSTRAINT_PK_NAME = "pk_records";
    static public String sqlBeginTx() {
        return "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE";
    }

    // TODO: Consider if this is better than just keeping "COMMIT" strings
    static public String sqlCommit() {
        return "COMMIT";
    }

    static public String sqlDropRecordTable() {
        return String.format("DROP TABLE IF EXISTS %s", TABLE_NAME);
    }

    static public String sqlCreateRecordTable() {
        return String.format("CREATE TABLE IF NOT EXISTS %s " +
                "(key TEXT CONSTRAINT %s PRIMARY KEY NOT NULL," +
                " owner TEXT NOT NULL," +
                " data BYTEA NOT NULL)", TABLE_NAME, CONSTRAINT_PK_NAME);
    }

    static public PreparedStatement stmtSelectRecord(Connection conn, String recordKey) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
                String.format("SELECT * FROM %s WHERE owner = ?", TABLE_NAME));
        stmt.setString(1, recordKey);
        return stmt;
    }

    static public PreparedStatement stmtInsertUpdateRecord(Connection conn, Record record) throws SQLException {
//        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder.append(String.format("INSERT INTO %s (key, owner, data)", TABLE_NAME));
//        stringBuilder.append(String.format(" VALUES (?, ?, ?)"));
        PreparedStatement stmt = conn.prepareStatement(
                String.format("INSERT INTO %s (key, owner, data)" +
                        " VALUES (?, ?, ?) ON CONFLICT ON CONSTRAINT %s DO UPDATE" +
                        " SET owner = ?, data = ? WHERE zktest_records.key = ?", TABLE_NAME, CONSTRAINT_PK_NAME));
        stmt.setString(1, record.getKey());
        stmt.setString(2, record.getOwner());
        stmt.setBytes(3, record.getData());
        stmt.setString(4, record.getOwner());
        stmt.setBytes(5, record.getData());
        stmt.setString(6, record.getKey());
        return stmt;
    }

    static public PreparedStatement stmtDeleteRecord(Connection conn, String recordKey) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
                String.format("DELETE FROM %s WHERE owner = ?", TABLE_NAME));
        stmt.setString(1, recordKey);
        return stmt;
    }

}
