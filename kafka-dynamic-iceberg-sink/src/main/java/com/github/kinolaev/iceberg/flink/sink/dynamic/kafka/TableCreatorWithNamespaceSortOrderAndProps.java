package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.TableCreator;
import org.apache.iceberg.util.PropertyUtil;

class TableCreatorWithNamespaceSortOrderAndProps implements TableCreator {
  private static final String TABLES_AUTO_CREATE_SORT_ORDER_BY_ID_COLUMNS_PROP =
      "iceberg.tables.auto-create-sort-order-by-id-columns";
  private static final boolean TABLES_AUTO_CREATE_SORT_ORDER_BY_ID_COLUMNS_DEFAULT = false;
  private static final String TABLES_AUTO_CREATE_PROPS_PREFIX = "iceberg.tables.auto-create-props.";

  private static final String TABLE_AUTO_CREATE_SORT_ORDER_BY_ID_COLUMNS_PROP =
      "iceberg.table.%s.auto-create-sort-order-by-id-columns";
  private static final String TABLE_AUTO_CREATE_PROPS_PREFIX =
      "iceberg.table.%s.auto-create-props.";

  private final Map<String, String> props;
  private final boolean defaultSortOrderByIdColumns;
  private final Map<String, String> defaultTableProps;

  TableCreatorWithNamespaceSortOrderAndProps(Map<String, String> props) {
    this.props = props;
    this.defaultSortOrderByIdColumns =
        PropertyUtil.propertyAsBoolean(
            props,
            TABLES_AUTO_CREATE_SORT_ORDER_BY_ID_COLUMNS_PROP,
            TABLES_AUTO_CREATE_SORT_ORDER_BY_ID_COLUMNS_DEFAULT);
    this.defaultTableProps =
        PropertyUtil.propertiesWithPrefix(props, TABLES_AUTO_CREATE_PROPS_PREFIX);
    ;
  }

  @Override
  public Table createTable(
      Catalog catalog, TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    ensureNamespace(catalog, identifier);

    return catalog
        .buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withSortOrder(sortOrderByIdColumns(identifier) ? sortOrder(schema) : SortOrder.unsorted())
        .withProperties(properties(identifier))
        .create();
  }

  private void ensureNamespace(Catalog catalog, TableIdentifier identifier) {
    if (identifier.hasNamespace()
        && catalog instanceof SupportsNamespaces catalogWithNamespaces
        && !catalogWithNamespaces.namespaceExists(identifier.namespace())) {
      catalogWithNamespaces.createNamespace(identifier.namespace());
    }
  }

  private boolean sortOrderByIdColumns(TableIdentifier identifier) {
    return PropertyUtil.propertyAsBoolean(
        props,
        TABLE_AUTO_CREATE_SORT_ORDER_BY_ID_COLUMNS_PROP.formatted(identifier),
        defaultSortOrderByIdColumns);
  }

  private SortOrder sortOrder(Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (int id : schema.identifierFieldIds()) {
      builder.asc(schema.findColumnName(id));
    }
    return builder.build();
  }

  private Map<String, String> properties(TableIdentifier identifier) {
    Map<String, String> tableProps =
        PropertyUtil.propertiesWithPrefix(
            props, TABLE_AUTO_CREATE_PROPS_PREFIX.formatted(identifier));
    defaultTableProps.forEach(tableProps::putIfAbsent);
    return tableProps;
  }
}
