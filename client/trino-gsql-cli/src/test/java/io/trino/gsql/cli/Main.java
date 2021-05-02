/*
 * Copyright (C) 2013 - 2020 Oracle and/or its affiliates. All rights reserved.
 */
package io.trino.gsql.cli;

import oracle.pgql.lang.Pgql;
import oracle.pgql.lang.PgqlException;
import oracle.pgql.lang.PgqlResult;

public class Main {
    public static final String DDL = "CREATE PROPERTY GRAPH financial_transactions\n" +
            "  VERTEX TABLES (\n" +
            "    Persons LABEL Person PROPERTIES ( name ),\n" +
            "    Companies LABEL Company PROPERTIES ( name ),\n" +
            "    Accounts LABEL Account PROPERTIES ( number )\n" +
            "  )\n" +
            "  EDGE TABLES (\n" +
            "    Transactions\n" +
            "      SOURCE KEY ( from_account ) REFERENCES Accounts\n" +
            "      DESTINATION KEY ( to_account ) REFERENCES Accounts\n" +
            "      LABEL transaction PROPERTIES ( amount ),\n" +
            "    Accounts AS PersonOwner\n" +
            "      SOURCE KEY ( number ) REFERENCES Accounts\n" +
            "      DESTINATION Persons\n" +
            "      LABEL owner NO PROPERTIES,\n" +
            "    Accounts AS CompanyOwner\n" +
            "      SOURCE KEY ( number ) REFERENCES Accounts\n" +
            "      DESTINATION Companies\n" +
            "      LABEL owner NO PROPERTIES,\n" +
            "    Persons AS worksFor\n" +
            "      SOURCE KEY ( id ) REFERENCES Persons\n" +
            "      DESTINATION Companies\n" +
            "      NO PROPERTIES\n" +
            "  )";

    public static void main(String[] args) throws PgqlException {

        try (Pgql pgql = new Pgql()) {

            // parse query and print graph query
            PgqlResult result1 = pgql.parse("SELECT n FROM MATCH (n:Person) -[e:likes]-> (m:Person) WHERE n.name = 'Dave'");
            System.out.println(result1.getPgqlStatement());

            // parse query with errors and print error messages
            PgqlResult result2 = pgql.parse("SELECT x, y FROM MATCH (n) -[e]-> (m)");
            System.out.println(result2.getErrorMessages());
            //parse DDL graph query
            PgqlResult result3 =pgql.parse(DDL);
            System.out.println(result3.getPgqlStatement());
        }
    }
}
