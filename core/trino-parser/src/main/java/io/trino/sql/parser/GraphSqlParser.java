package io.trino.sql.parser;

import io.trino.sql.tree.Statement;

public class GraphSqlParser implements StatementCreator {
    @Override
    public Statement createStatement(String sql, ParsingOptions parsingOptions) {
        return null;
    }

    @Override
    public boolean isParse(String sql) {
        return false;
    }
}
