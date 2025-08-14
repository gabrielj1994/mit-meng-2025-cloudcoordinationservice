package org.example.storage.rdb;

import org.example.model.Record;
import org.example.storage.DataStore;
import org.example.util.PSQLUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PSQLStore implements DataStore {

    public PSQLStore() {
        initialize();
    }

    private void initialize() {
        try (Connection conn = ConnectionPool.getConnection();) {
            Statement stmt = conn.createStatement();
            Statement tx_stmt = conn.createStatement();
            tx_stmt.execute(PSQLUtil.sqlBeginTx());
            // TODO: Consider dropping table for clean init
//            stmt.execute(PSQLUtil.sqlDropRecordTable());
            stmt.execute(PSQLUtil.sqlCreateRecordTable());

            tx_stmt.execute(PSQLUtil.sqlCommit());
        } catch (SQLException e) {
            // TODO: retry a few times
            System.out.println(String.format("Error initializing log table."));
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean updateRecord(Record record) {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();
            tx_stmt.execute(PSQLUtil.sqlBeginTx());
            boolean result = PSQLUtil.stmtInsertUpdateRecord(conn, record).executeUpdate() > 0;
            tx_stmt.execute(PSQLUtil.sqlCommit());
            return result;
        } catch (SQLException e) {
            // TODO: Consider not throwing and returning false
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean replaceRecords(List<Record> records) {
        return true;
    }

    @Override
    public boolean removeRecord(String recordKey) {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();
            tx_stmt.execute(PSQLUtil.sqlBeginTx());
            // TODO: Consider if anything needs to be tracked/logged for delete
            PSQLUtil.stmtDeleteRecord(conn, recordKey).executeUpdate();
            tx_stmt.execute(PSQLUtil.sqlCommit());
            return true;
        } catch (SQLException e) {
            // TODO: Consider not throwing and returning false
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Record> getRecord(String recordKey) {
        try (Connection conn = ConnectionPool.getConnection()) {
            Record resultRecord = null;
            Statement tx_stmt = conn.createStatement();

            tx_stmt.execute(PSQLUtil.sqlBeginTx());
            ResultSet rs = PSQLUtil.stmtSelectRecord(conn, recordKey).executeQuery();
            if (rs.next()) {
                resultRecord = new Record(
                        recordKey,
                        rs.getString("owner"),
                        rs.getBytes("data"));
            }
            tx_stmt.execute(PSQLUtil.sqlCommit());
            return Optional.ofNullable(resultRecord);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Record> getAllRecords() {
        return new ArrayList<>();
    }
}
