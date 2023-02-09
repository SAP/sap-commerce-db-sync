/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

package com.sap.cx.boosters.commercedbsync.repository.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cx.boosters.commercedbsync.MarkersQueryDefinition;
import com.sap.cx.boosters.commercedbsync.OffsetQueryDefinition;
import com.sap.cx.boosters.commercedbsync.SeekQueryDefinition;
import com.sap.cx.boosters.commercedbsync.dataset.DataSet;
import com.sap.cx.boosters.commercedbsync.profile.DataSourceConfiguration;
import com.sap.cx.boosters.commercedbsync.service.DatabaseMigrationDataTypeMapperService;

import de.hybris.platform.util.Config;

public class MySQLIncrementalDataRepository extends MySQLDataRepository{

  private static final Logger LOG = LoggerFactory.getLogger(MySQLIncrementalDataRepository.class);

  private static String  deletionTable = Config.getParameter("db.tableprefix") == null ? "" : Config.getParameter("db.tableprefix")+ "itemdeletionmarkers";

  public MySQLIncrementalDataRepository(
      DataSourceConfiguration dataSourceConfiguration,
      DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
    super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
  }
  @Override
  protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {

    if(!queryDefinition.isDeletionEnabled()) {
      return super.buildOffsetBatchQuery(queryDefinition,conditions);
    }
    return String.format("select * from %s where %s order by %s limit %s,%s", deletionTable, expandConditions(conditions), queryDefinition.getOrderByColumns(), queryDefinition.getOffset(), queryDefinition.getBatchSize());
  }

  @Override
  protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
    if(!queryDefinition.isDeletionEnabled()) {
      return super.buildValueBatchQuery(queryDefinition,conditions);
    }
    return String.format("select * from %s where %s order by %s limit %s", deletionTable, expandConditions(conditions), queryDefinition.getColumn(), queryDefinition.getBatchSize());
  }

  @Override
  protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
    if(!queryDefinition.isDeletionEnabled()) {
      return super.buildBatchMarkersQuery(queryDefinition,conditions);
    }
    String column = queryDefinition.getColumn();
    return String.format("SELECT %s,rownum\n" +
        "FROM ( \n" +
        "    SELECT \n" +
        "        @row := @row +1 AS rownum, %s \n" +
        "    FROM (SELECT @row :=-1) r, %s  WHERE %s ORDER BY %s) ranked \n" +
        "WHERE rownum %% %s = 0 ", column, column, deletionTable, expandConditions(conditions), column, queryDefinition.getBatchSize());
  }




  @Override
  public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition, Instant time) throws Exception {
    //
    if(!queryDefinition.isDeletionEnabled()) {
      return super.getBatchOrderedByColumn(queryDefinition,time);
    }

    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(3);
    processDefaultConditions(queryDefinition.getTable(), conditionsList);
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

    if(!queryDefinition.isDeletionEnabled()) {
      return super.getBatchWithoutIdentifier(queryDefinition,time);
    }
    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(2);
    processDefaultConditions(queryDefinition.getTable(), conditionsList);
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
  public long getRowCountModifiedAfter(String table, Instant time,boolean isDeletionEnabled,boolean lpTableMigrationEnabled) throws SQLException {
    if(!isDeletionEnabled) {
      return super.getRowCountModifiedAfter(table,time,false,false);
    }
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

}
