/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.sql.analyzer.Output;
import io.trino.sql.parser.GraphSqlParser;
import io.trino.sql.tree.CreatePropertyGraph;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;
import java.util.*;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.sql.ParsingUtil.createParsingOptions;

public class CreatePropertyGraphTask
        implements DataDefinitionTask<CreatePropertyGraph> {
    private GraphSqlParser gsqlParser;

    @Inject
    public CreatePropertyGraphTask(GraphSqlParser gsqlParser) {
        this.gsqlParser = gsqlParser;
    }

    @Override
    public String getName() {
        return "CREATE PROPERTY GRAPH   ";
    }

    @Override
    public String explain(CreatePropertyGraph statement, List<Expression> parameters) {
        return "CREATE PROPERTY GRAPH " + statement.getName();
    }

    @Override
    public ListenableFuture<?> execute(
            CreatePropertyGraph statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector) {
        return internalExecute(statement, metadata, accessControl, stateMachine.getSession(), parameters, output -> stateMachine.setOutput(Optional.of(output)));
    }

    //todo - implement correctly
    @VisibleForTesting
    ListenableFuture<?> internalExecute(CreatePropertyGraph statement, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, Consumer<Output> outputConsumer) {
        checkArgument(!statement.getElements().isEmpty(), "no elements for graph");
        gsqlParser.createStatement(statement.toString(), createParsingOptions(session));
/*
        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (tableHandle.isPresent()) {
            if (!statement.isNotExists()) {
                throw semanticException(TABLE_ALREADY_EXISTS, statement, "Graph '%s' already exists", tableName);
            }
            return immediateFuture(null);
        }

        CatalogName catalogName = metadata.getCatalogHandle(session, tableName.getCatalogName())
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + tableName.getCatalogName()));
*/

 /*
        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<>();
        Map<String, Object> inheritedProperties = ImmutableMap.of();
        boolean includingProperties = false;
        for (GraphElement element : statement.getElements()) {
            if (element instanceof ColumnDefinition) {
                ColumnDefinition column = (ColumnDefinition) element;
                String name = column.getName().getValue().toLowerCase(Locale.ENGLISH);
                Type type;
                try {
                    type = metadata.getType(toTypeSignature(column.getType()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_NOT_FOUND, element, "Unknown type '%s' for column '%s'", column.getType(), column.getName());
                }
                if (type.equals(UNKNOWN)) {
                    throw semanticException(COLUMN_TYPE_UNKNOWN, element, "Unknown type '%s' for column '%s'", column.getType(), column.getName());
                }
                if (columns.containsKey(name)) {
                    throw semanticException(DUPLICATE_COLUMN_NAME, column, "Column name '%s' specified more than once", column.getName());
                }
                if (!column.isNullable() && !metadata.getConnectorCapabilities(session, catalogName).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
                    throw semanticException(NOT_SUPPORTED, column, "Catalog '%s' does not support non-null column for column name '%s'", catalogName.getCatalogName(), column.getName());
                }

                Map<String, Expression> sqlProperties = mapFromProperties(column.getProperties());
                Map<String, Object> columnProperties = metadata.getColumnPropertyManager().getProperties(
                        catalogName,
                        tableName.getCatalogName(),
                        sqlProperties,
                        session,
                        metadata,
                        accessControl,
                        parameterLookup);

                columns.put(name, ColumnMetadata.builder()
                        .setName(name)
                        .setType(type)
                        .setNullable(column.isNullable())
                        .setComment(column.getComment())
                        .setProperties(columnProperties)
                        .build());
            }
            else if (element instanceof LikeClause) {
                LikeClause likeClause = (LikeClause) element;
                QualifiedObjectName likeTableName = createQualifiedObjectName(session, statement, likeClause.getTableName());
                if (metadata.getCatalogHandle(session, likeTableName.getCatalogName()).isEmpty()) {
                    throw semanticException(CATALOG_NOT_FOUND, statement, "LIKE table catalog '%s' does not exist", likeTableName.getCatalogName());
                }
                if (!tableName.getCatalogName().equals(likeTableName.getCatalogName())) {
                    throw semanticException(NOT_SUPPORTED, statement, "LIKE table across catalogs is not supported");
                }
                TableHandle likeTable = metadata.getTableHandle(session, likeTableName)
                        .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, statement, "LIKE table '%s' does not exist", likeTableName));

                TableMetadata likeTableMetadata = metadata.getTableMetadata(session, likeTable);

                Optional<LikeClause.PropertiesOption> propertiesOption = likeClause.getPropertiesOption();
                if (propertiesOption.isPresent() && propertiesOption.get() == LikeClause.PropertiesOption.INCLUDING) {
                    if (includingProperties) {
                        throw semanticException(NOT_SUPPORTED, statement, "Only one LIKE clause can specify INCLUDING PROPERTIES");
                    }
                    includingProperties = true;
                    inheritedProperties = likeTableMetadata.getMetadata().getProperties();
                }

                try {
                    accessControl.checkCanSelectFromColumns(
                            session.toSecurityContext(),
                            likeTableName,
                            likeTableMetadata.getColumns().stream()
                                    .map(ColumnMetadata::getName)
                                    .collect(toImmutableSet()));
                }
                catch (AccessDeniedException e) {
                    throw new AccessDeniedException("Cannot reference columns of table " + likeTableName);
                }
                if (propertiesOption.orElse(EXCLUDING) == INCLUDING) {
                    try {
                        accessControl.checkCanShowCreateTable(session.toSecurityContext(), likeTableName);
                    }
                    catch (AccessDeniedException e) {
                        throw new AccessDeniedException("Cannot reference properties of table " + likeTableName);
                    }
                }

                likeTableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .forEach(column -> {
                            if (columns.containsKey(column.getName().toLowerCase(Locale.ENGLISH))) {
                                throw semanticException(DUPLICATE_COLUMN_NAME, element, "Column name '%s' specified more than once", column.getName());
                            }
                            columns.put(column.getName().toLowerCase(Locale.ENGLISH), column);
                        });
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid TableElement: " + element.getClass().getName());
            }
        }

        accessControl.checkCanCreateTable(session.toSecurityContext(), tableName);

        Map<String, Expression> sqlProperties = mapFromProperties(statement.getEdges());
        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                catalogName,
                tableName.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                accessControl,
                parameterLookup);

        Map<String, Object> finalProperties = combineProperties(sqlProperties.keySet(), properties, inheritedProperties);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.copyOf(columns.values()), Collections.EMPTY_MAP, statement.getComment());
        try {
            metadata.createTable(session, tableName.getCatalogName(), tableMetadata, statement.isNotExists());
        }
        catch (TrinoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }
        outputConsumer.accept(new Output(
                tableName.getCatalogName(),
                tableName.getSchemaName(),
                tableName.getObjectName(),
                Optional.of(tableMetadata.getColumns().stream()
                        .map(column -> new OutputColumn(new Column(column.getName(), column.getType().toString()), ImmutableSet.of()))
                        .collect(toImmutableList()))));
*/
        return immediateFuture(null);
    }

    private static Map<String, Object> combineProperties(Set<String> specifiedPropertyKeys, Map<String, Object> defaultProperties, Map<String, Object> inheritedProperties) {
        Map<String, Object> finalProperties = new HashMap<>(inheritedProperties);
        for (Map.Entry<String, Object> entry : defaultProperties.entrySet()) {
            if (specifiedPropertyKeys.contains(entry.getKey()) || !finalProperties.containsKey(entry.getKey())) {
                finalProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return finalProperties;
    }
}
