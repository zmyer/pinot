package com.linkedin.thirdeye.datalayer.util;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.thirdeye.datalayer.entity.AbstractEntity;
import com.linkedin.thirdeye.datalayer.entity.AbstractJsonEntity;
import com.linkedin.thirdeye.db.entity.AbstractBaseEntity;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EntityMappingHolder {
  //Map<TableName,EntityName>
  BiMap<String, String> tableToEntityNameMap = HashBiMap.create();
  Map<String, LinkedHashMap<String, ColumnInfo>> columnInfoPerTable = new HashMap<>();
  //DB NAME to ENTITY NAME mapping
  Map<String, BiMap<String, String>> columnMappingPerTable = new HashMap<>();

  public void register(Connection connection, Class<? extends AbstractEntity> entityClass,
      String tableName) throws Exception {
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    String catalog = null;
    String schemaPattern = null;
    String columnNamePattern = null;
    //listTables(databaseMetaData, catalog, schemaPattern);
    tableToEntityNameMap.put(tableName, entityClass.getSimpleName());
    columnMappingPerTable.put(tableName, HashBiMap.create());
    LinkedHashMap<String, ColumnInfo> columnInfoMap = new LinkedHashMap<>();
    //try both upper case and lower case 
    String[] tableNamePatterns = new String[] {tableName.toLowerCase(), tableName.toUpperCase()};
    for (String tableNamePattern : tableNamePatterns) {
      ResultSet rs =
          databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
      while (rs.next()) {
        String columnName = rs.getString(4);
        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.columnNameInDB = columnName.toLowerCase();
        columnInfo.sqlType = rs.getInt(5);
        columnInfoMap.put(columnName.toLowerCase(), columnInfo);
      }
    }
    List<Field> fields = new ArrayList<>();
    getAllFields(fields, entityClass);
    for (Field field : fields) {
      field.setAccessible(true);
      String entityColumn = field.getName();
      for (String dbColumn : columnInfoMap.keySet()) {
        boolean success = false;
        if (dbColumn.toLowerCase().equals(entityColumn.toLowerCase())) {
          success = true;
        }
        String dbColumnNormalized = dbColumn.replaceAll("_", "").toLowerCase();
        String entityColumnNormalized = entityColumn.replaceAll("_", "").toLowerCase();
        if (dbColumnNormalized.equals(entityColumnNormalized)) {
          success = true;
        }
        if (success) {
          columnInfoMap.get(dbColumn).columnNameInEntity = entityColumn;
          columnInfoMap.get(dbColumn).field = field;
          System.out.println("Mapped " + dbColumn + " to " + entityColumn);
          columnMappingPerTable.get(tableName).put(dbColumn, entityColumn);
        }
      }
    }
    columnInfoPerTable.put(tableName, columnInfoMap);
  }

  private void listTables(DatabaseMetaData databaseMetaData, String catalog, String schemaPattern)
      throws SQLException {
    ResultSet tablesResultSet = databaseMetaData.getTables(catalog, schemaPattern, "%", null);
    while (tablesResultSet.next()) {
      for (int i = 1; i <= tablesResultSet.getMetaData().getColumnCount(); i++) {
        System.out.print(tablesResultSet.getString(i) + ", ");
      }
      System.out.println("");
    }
  }

  public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
    fields.addAll(Arrays.asList(type.getDeclaredFields()));
    if (type.getSuperclass() != null) {
      fields = getAllFields(fields, type.getSuperclass());
    }
    return fields;
  }

}


class TableInfo {
  String dbTableName;
  String className;

  String getDbTableName(Class<? extends AbstractJsonEntity> clazz) {
    return null;
  }

}


class ColumnInfo {
  String columnNameInDB;
  int sqlType;
  String columnNameInEntity;
  Field field;
}
