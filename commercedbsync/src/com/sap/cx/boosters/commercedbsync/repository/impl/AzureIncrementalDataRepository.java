/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import com.google.common.base.Joiner;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;
import de.hybris.platform.util.Config;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureIncrementalDataRepository extends AzureDataRepository{

  private static final Logger LOG = LoggerFactory.getLogger(AzureIncrementalDataRepository.class);

  private static final String LP_SUFFIX = "lp";

  private static final String PK = "PK";

  private static String  deletionTable = Config.getParameter("db.tableprefix") == null ? "" : Config.getParameter("db.tableprefix")+ "itemdeletionmarkers";

  public AzureIncrementalDataRepository(
      DataSourceConfiguration dataSourceConfiguration,
      DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
    super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
  }
  @Override
  protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {

    if(queryDefinition.isDeletionEnabled()) {
      return buildOffsetBatchQueryForDeletion(queryDefinition,conditions);
    } else if(queryDefinition.isLpTableEnabled()) {
      return buildOffsetBatchQueryForLp(queryDefinition,conditions);
    }
    else {
      return super.buildOffsetBatchQuery(queryDefinition,conditions);
    }
  }

  private String buildOffsetBatchQueryForLp(OffsetQueryDefinition queryDefinition, String... conditions) {
    String orderBy = PK;
    return String.format("SELECT * FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY", getLpTableName(queryDefinition.getTable()), expandConditions(conditions), orderBy, queryDefinition.getOffset(), queryDefinition.getBatchSize());
  }

  private String buildOffsetBatchQueryForDeletion(OffsetQueryDefinition queryDefinition, String... conditions) {
    return String.format("SELECT * FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY", deletionTable, expandConditions(conditions), queryDefinition.getOrderByColumns(), queryDefinition.getOffset(), queryDefinition.getBatchSize());
  }

  @Override
  protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
    if(queryDefinition.isDeletionEnabled()) {
      return buildValueBatchQueryForDeletion(queryDefinition,conditions);
    } else {
      return super.buildValueBatchQuery(queryDefinition,conditions);
    }
  }

  @Override
  protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
    if(queryDefinition.isDeletionEnabled()) {
      return buildBatchMarkersQueryForDeletion(queryDefinition,conditions);
    } else if(queryDefinition.isLpTableEnabled()) {
      return super.buildBatchMarkersQuery(queryDefinition,conditions);
    } else {
       return super.buildBatchMarkersQuery(queryDefinition,conditions);
    }
  }

  @Override
  public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition, Instant time) throws Exception {
    if(queryDefinition.isDeletionEnabled()) {
      return getBatchOrderedByColumnForDeletion(queryDefinition,time);
    } else if(queryDefinition.isLpTableEnabled()){
      return getBatchOrderedByColumnForLptable(queryDefinition,time);
    } else {
      return super.getBatchOrderedByColumn(queryDefinition,time);
    }
  }

  private String buildValueBatchQueryForDeletion(SeekQueryDefinition queryDefinition, String... conditions) {
    return String.format("select top %s * from %s where %s order by %s", queryDefinition.getBatchSize(), deletionTable, expandConditions(conditions), queryDefinition.getColumn());
  }

  private DataSet getBatchOrderedByColumnForLptable(SeekQueryDefinition queryDefinition, Instant time) throws Exception {
    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(2);
    processDefaultConditions(queryDefinition.getTable(), conditionsList);
    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    if (queryDefinition.getLastColumnValue() != null) {
      conditionsList.add(String.format("%s >= %s", queryDefinition.getColumn(), queryDefinition.getLastColumnValue()));
    }
    if (queryDefinition.getNextColumnValue() != null) {
      conditionsList.add(String.format("%s < %s", queryDefinition.getColumn(), queryDefinition.getNextColumnValue()));
    }
    String[] conditions = null;
    List<String> pkList = new ArrayList<String>();
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connectionForPk = getConnection();
        PreparedStatement stmt = connectionForPk.prepareStatement(buildValueBatchQueryForLptable(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      ResultSet pkResultSet = stmt.executeQuery();
       pkList = convertToPkListForLpTable(pkResultSet);
    }

    // migrating LP Table no
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildValueBatchQueryForLptableWithPK(queryDefinition,pkList, conditions))) {
      // stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }
  }

  private List<String> convertToPkListForLpTable(ResultSet resultSet) throws Exception {
    List<String> pkList = new ArrayList<>();
    while (resultSet.next()) {
        int idx = resultSet.findColumn(PK);
         pkList.add(resultSet.getString(idx));
    }
    return pkList;
  }

  private String buildValueBatchQueryForLptableWithPK(SeekQueryDefinition queryDefinition, List<String> pkList, String... conditions ) {

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(String.format("select  * from %s where ", queryDefinition.getTable()));
    sqlBuilder.append("\n");
    sqlBuilder.append(String.format("ITEMPK in (%s) " , Joiner.on(',').join(pkList.stream().map(column -> " " + column).collect(Collectors.toList()))));
    sqlBuilder.append(String.format("%s order by %s ", expandConditions(conditions), queryDefinition.getColumn()));
    sqlBuilder.append(";");

    return sqlBuilder.toString();
  }

  private String buildValueBatchQueryForLptableWithPK(OffsetQueryDefinition queryDefinition, List<String> pkList, String... conditions ) {

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(String.format("select  * from %s where ", queryDefinition.getTable()));
    sqlBuilder.append("\n");
    sqlBuilder.append(String.format("ITEMPK in (%s) " , Joiner.on(',').join(pkList.stream().map(column -> " " + column).collect(Collectors.toList()))));
    sqlBuilder.append(";");

    return sqlBuilder.toString();
  }
  private String buildValueBatchQueryForLptable(SeekQueryDefinition queryDefinition, String... conditions) {
    return String.format("select top %s PK from %s where %s order by %s", queryDefinition.getBatchSize(), getLpTableName(queryDefinition.getTable()), expandConditions(conditions), queryDefinition.getColumn());
  }

  private String buildOffsetBatchQueryForLptable(OffsetQueryDefinition queryDefinition, String... conditions) {
    String orderBy = PK;
    return String.format("SELECT PK FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY", getLpTableName(queryDefinition.getTable()), expandConditions(conditions), orderBy, queryDefinition.getOffset(), queryDefinition.getBatchSize());
  }

  private DataSet getBatchOrderedByColumnForDeletion(SeekQueryDefinition queryDefinition, Instant time) throws Exception {

    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(3);
    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    conditionsList.add("p_table = ?");
    if (queryDefinition.getLastColumnValue() != null) {
      conditionsList.add(String.format("%s >= %s", queryDefinition.getColumn(), queryDefinition.getLastColumnValue()));
    }
    if (queryDefinition.getNextColumnValue() != null) {
      conditionsList.add(String.format("%s < %s", queryDefinition.getColumn(), queryDefinition.getNextColumnValue()));
    }
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildValueBatchQuery(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      // setting table for the deletions
      stmt.setString(2,queryDefinition.getTable());

      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }
  }

  @Override
  public DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition, Instant time) throws Exception {

    if(queryDefinition.isDeletionEnabled()) {
      return getBatchWithoutIdentifierForDeletion(queryDefinition,time);
    } else if(queryDefinition.isLpTableEnabled()){
      return getBatchWithoutIdentifierForLptable(queryDefinition,time);
    } else {
      return super.getBatchWithoutIdentifier(queryDefinition,time);
    }
  }

  private DataSet getBatchWithoutIdentifierForDeletion(OffsetQueryDefinition queryDefinition, Instant time) throws Exception {
    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(2);

    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    conditionsList.add("p_table = ?");
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildOffsetBatchQuery(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      // setting table for the deletions
      stmt.setString(2,queryDefinition.getTable());

      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }
  }

  private DataSet getBatchWithoutIdentifierForLptable(OffsetQueryDefinition queryDefinition, Instant time) throws Exception {
    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(1);
    processDefaultConditions(queryDefinition.getTable(), conditionsList);
    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    List<String> pkList = new ArrayList<String>();
    try (Connection connectionForPk = getConnection();
        PreparedStatement stmt = connectionForPk.prepareStatement(buildOffsetBatchQueryForLptable(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      ResultSet pkResultSet = stmt.executeQuery();
      pkList = convertToPkListForLpTable(pkResultSet);
    }

    // migrating LP Table no
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildValueBatchQueryForLptableWithPK(queryDefinition,pkList, conditions))) {
      // stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }

  }

  @Override
  public DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition, Instant time) throws Exception {

    if(!queryDefinition.isDeletionEnabled()) {
      return super.getBatchMarkersOrderedByColumn(queryDefinition,time);
    }
    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(2);
    processDefaultConditions(queryDefinition.getTable(), conditionsList);
    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    // setting table for the deletions
    conditionsList.add("p_table = ?");

    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildBatchMarkersQuery(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      // setting table for the deletions
      stmt.setString(2,queryDefinition.getTable());

      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }
  }

  @Override
  public long getRowCountModifiedAfter(String table, Instant time,boolean isDeletionEnabled, boolean lpTableMigrationEnabled) throws SQLException {
    if(isDeletionEnabled) {
      return getRowCountModifiedAfterforDeletion(table,time);
    } else if(lpTableMigrationEnabled) {
      return getRowCountModifiedAfterforLpTable(table,time);
    }
    else{
      return super.getRowCountModifiedAfter(table,time,false,false);

    }
  }

  private long getRowCountModifiedAfterforLpTable(String table, Instant time) throws SQLException {
    List<String> conditionsList = new ArrayList<>(1);
    processDefaultConditions(table, conditionsList);
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection()) {
      try (PreparedStatement stmt = connection.prepareStatement(String.format("select count(*) from %s where modifiedts > ? AND %s", getLpTableName(table), expandConditions(conditions)))) {
        stmt.setTimestamp(1, Timestamp.from(time));
        ResultSet resultSet = stmt.executeQuery();
        long value = 0;
        if (resultSet.next()) {
          value = resultSet.getLong(1);
        }
        return value;
      }
    }
  }

  private long getRowCountModifiedAfterforDeletion(String table, Instant time) throws SQLException {
    //
    List<String> conditionsList = new ArrayList<>(2);
    processDefaultConditions(table, conditionsList);
    // setting table for the deletions
    conditionsList.add("p_table = ?");
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection()) {
      try (PreparedStatement stmt = connection.prepareStatement(String.format("select count(*) from %s where modifiedts > ? AND %s", deletionTable, expandConditions(conditions)))) {
        stmt.setTimestamp(1, Timestamp.from(time));
        // setting table for the deletions
        stmt.setString(2,table);
        ResultSet resultSet = stmt.executeQuery();
        long value = 0;
        if (resultSet.next()) {
          value = resultSet.getLong(1);
        }
        return value;
      }
    }
  }

  private String buildBatchMarkersQueryForDeletion(MarkersQueryDefinition queryDefinition, String... conditions) {
    String column = queryDefinition.getColumn();
    return String.format("SELECT t.%s, t.rownum\n" +
        "FROM\n" +
        "(\n" +
        "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownum\n" +
        "    FROM %s\n WHERE %s" +
        ") AS t\n" +
        "WHERE t.rownum %% %s = 0\n" +
        "ORDER BY t.%s", column, column, column, deletionTable, expandConditions(conditions), queryDefinition.getBatchSize(), column);
  }

  private long getRowCountModifiedAfterForLP(String table, Instant time) throws SQLException {
    List<String> conditionsList = new ArrayList<>(1);

    if (! StringUtils.endsWithIgnoreCase(table,LP_SUFFIX)) {
      return super.getRowCountModifiedAfter(table,time,false,false);
    }
    table = StringUtils.removeEndIgnoreCase(table,LP_SUFFIX);

    processDefaultConditions(table, conditionsList);
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection()) {
      try (PreparedStatement stmt = connection.prepareStatement(String.format("select count(*) from %s where modifiedts > ? AND %s", table, expandConditions(conditions)))) {
        stmt.setTimestamp(1, Timestamp.from(time));
        ResultSet resultSet = stmt.executeQuery();
        long value = 0;
        if (resultSet.next()) {
          value = resultSet.getLong(1);
        }
        return value;
      }
    }
  }

  private String getLpTableName(String tableName){
    return StringUtils.removeEndIgnoreCase(tableName,LP_SUFFIX);
  }
  }
