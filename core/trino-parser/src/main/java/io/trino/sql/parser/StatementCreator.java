package io.trino.sql.parser;

import io.trino.sql.tree.Statement;

public interface StatementCreator {
    Statement createStatement(String sql, ParsingOptions parsingOptions);
    boolean isParse(String sql);
}
