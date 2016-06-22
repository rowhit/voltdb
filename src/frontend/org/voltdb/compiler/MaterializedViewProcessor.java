/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.IndexRef;
import org.voltdb.catalog.MaterializedViewHandlerInfo;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TableRef;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class MaterializedViewProcessor {

    private final VoltCompiler m_compiler;
    private final HSQLInterface m_hsql;

    public MaterializedViewProcessor(VoltCompiler compiler,
                                     HSQLInterface hsql) {
        assert(compiler != null);
        assert(hsql != null);
        m_compiler = compiler;
        m_hsql = hsql;
    }

    /**
     * Add materialized view info to the catalog for the tables that are
     * materialized views.
     * @throws VoltCompilerException
     */
    public void startProcessing(Database db, HashMap<Table, String> matViewMap, TreeSet<String> exportTableNames)
            throws VoltCompilerException {
        HashSet <String> viewTableNames = new HashSet<>();
        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            viewTableNames.add(entry.getKey().getTypeName());
        }

        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            Table destTable = entry.getKey();
            String query = entry.getValue();

            // get the xml for the query
            VoltXMLElement xmlquery = null;
            try {
                xmlquery = m_hsql.getXMLCompiledStatement(query);
            }
            catch (HSQLParseException e) {
                e.printStackTrace();
            }
            assert(xmlquery != null);

            // parse the xml like any other sql statement
            ParsedSelectStmt stmt = null;
            try {
                stmt = (ParsedSelectStmt) AbstractParsedStmt.parse(query, xmlquery, null, db, null);
            }
            catch (Exception e) {
                throw m_compiler.new VoltCompilerException(e.getMessage());
            }
            assert(stmt != null);

            String viewName = destTable.getTypeName();
            // throw an error if the view isn't within voltdb's limited world view
            checkViewMeetsSpec(viewName, stmt);

            // Allow only non-unique indexes other than the primary key index.
            // The primary key index is yet to be defined (below).
            for (Index destIndex : destTable.getIndexes()) {
                if (destIndex.getUnique() || destIndex.getAssumeunique()) {
                    String msg = "A UNIQUE or ASSUMEUNIQUE index is not allowed on a materialized view. " +
                                 "Remove the qualifier from the index " + destIndex.getTypeName() +
                                 "defined on the materialized view \"" + viewName + "\".";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }

            // Add mvHandlerInfo to the destTable:
            MaterializedViewHandlerInfo mvHandlerInfo = destTable.getMvhandlerinfo().add("mvHandlerInfo");
            if (stmt.m_tableList.size() > 1) {
                mvHandlerInfo.setIsjoinedtableview(true);
            }
            for (Table srcTable : stmt.m_tableList) {
                if (viewTableNames.contains(srcTable.getTypeName())) {
                    String msg = String.format("A materialized view (%s) can not be defined on another view (%s).",
                            viewName, srcTable.getTypeName());
                    throw m_compiler.new VoltCompilerException(msg);
                }
                // Maybe in the future all materialized views will use the query plan?
                if (mvHandlerInfo.getIsjoinedtableview() && exportTableNames.contains(srcTable.getTypeName())) {
                    String msg = String.format("A materialized view (%s) on joined tables cannot have streamed table (%s) as its source.",
                                               viewName, srcTable.getTypeName());
                    throw m_compiler.new VoltCompilerException(msg);
                }
                // Add the reference to destTable to the affectedViewTables of each source table.
                TableRef tableRef = srcTable.getAffectedviewtables().add(destTable.getTypeName());
                tableRef.setTable(destTable);
            }

            // create the materializedviewinfo catalog node for the source table
            Table srcTable = stmt.m_tableList.get(0);
            MaterializedViewInfo matviewinfo = srcTable.getViews().add(viewName);
            matviewinfo.setDest(destTable);
            AbstractExpression where = stmt.getSingleTableFilterExpression();
            if (where != null) {
                String hex = Encoder.hexEncode(where.toJSONString());
                matviewinfo.setPredicate(hex);
            }
            else {
                matviewinfo.setPredicate("");
            }
            destTable.setMaterializer(srcTable);

            List<Column> srcColumnArray = CatalogUtil.getSortedCatalogItems(srcTable.getColumns(), "index");
            List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(destTable.getColumns(), "index");
            List<AbstractExpression> groupbyExprs = null;

            if (stmt.hasComplexGroupby()) {
                groupbyExprs = new ArrayList<AbstractExpression>();
                for (ParsedColInfo col: stmt.m_groupByColumns) {
                    groupbyExprs.add(col.expression);
                }
                // Parse group by expressions to json string
                String groupbyExprsJson = null;
                try {
                    groupbyExprsJson = DDLCompiler.convertToJSONArray(groupbyExprs);
                } catch (JSONException e) {
                    throw m_compiler.new VoltCompilerException ("Unexpected error serializing non-column " +
                            "expressions for group by expressions: " + e.toString());
                }
                matviewinfo.setGroupbyexpressionsjson(groupbyExprsJson);
            }
            else {
                // add the group by columns from the src table
                for (int i = 0; i < stmt.m_groupByColumns.size(); i++) {
                    ParsedColInfo gbcol = stmt.m_groupByColumns.get(i);
                    Column srcCol = srcColumnArray.get(gbcol.index);
                    ColumnRef cref = matviewinfo.getGroupbycols().add(srcCol.getTypeName());
                    // groupByColumns is iterating in order of groups. Store that grouping order
                    // in the column ref index. When the catalog is serialized, it will, naturally,
                    // scramble this order like a two year playing dominos, presenting the data
                    // in a meaningless sequence.
                    cref.setIndex(i);           // the column offset in the view's grouping order
                    cref.setColumn(srcCol);     // the source column from the base (non-view) table
                }

                // parse out the group by columns into the dest table
                for (int i = 0; i < stmt.m_groupByColumns.size(); i++) {
                    ParsedColInfo col = stmt.m_displayColumns.get(i);
                    Column destColumn = destColumnArray.get(i);
                    processMaterializedViewColumn(matviewinfo, srcTable, destColumn,
                            ExpressionType.VALUE_TUPLE, (TupleValueExpression)col.expression);
                }
            }

            // Set up COUNT(*) column
            ParsedColInfo countCol = stmt.m_displayColumns.get(stmt.m_groupByColumns.size());
            assert(countCol.expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR);
            assert(countCol.expression.getLeft() == null);
            processMaterializedViewColumn(matviewinfo, srcTable,
                    destColumnArray.get(stmt.m_groupByColumns.size()),
                    ExpressionType.AGGREGATE_COUNT_STAR, null);

            // create an index and constraint for the table
            // After ENG-7872 is fixed if there is no group by column then we will not create any
            // index or constraint in order to avoid error and crash.
            if (stmt.m_groupByColumns.size() != 0) {
                Index pkIndex = destTable.getIndexes().add(HSQLInterface.AUTO_GEN_MATVIEW_IDX);
                pkIndex.setType(IndexType.BALANCED_TREE.getValue());
                pkIndex.setUnique(true);
                // add the group by columns from the src table
                // assume index 1 throuh #grpByCols + 1 are the cols
                for (int i = 0; i < stmt.m_groupByColumns.size(); i++) {
                    ColumnRef c = pkIndex.getColumns().add(String.valueOf(i));
                    c.setColumn(destColumnArray.get(i));
                    c.setIndex(i);
                }
                Constraint pkConstraint = destTable.getConstraints().add(HSQLInterface.AUTO_GEN_MATVIEW_CONST);
                pkConstraint.setType(ConstraintType.PRIMARY_KEY.getValue());
                pkConstraint.setIndex(pkIndex);
            }

            // prepare info for aggregation columns.
            List<AbstractExpression> aggregationExprs = new ArrayList<AbstractExpression>();
            boolean hasAggregationExprs = false;
            ArrayList<AbstractExpression> minMaxAggs = new ArrayList<AbstractExpression>();
            for (int i = stmt.m_groupByColumns.size() + 1; i < stmt.m_displayColumns.size(); i++) {
                ParsedColInfo col = stmt.m_displayColumns.get(i);
                AbstractExpression aggExpr = col.expression.getLeft();
                if (aggExpr.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                    hasAggregationExprs = true;
                }
                aggregationExprs.add(aggExpr);
                if (col.expression.getExpressionType() ==  ExpressionType.AGGREGATE_MIN ||
                        col.expression.getExpressionType() == ExpressionType.AGGREGATE_MAX) {
                    minMaxAggs.add(aggExpr);
                }
            }

            // Generate query XMLs for min/max recalculation (ENG-8641)
            MatViewFallbackQueryXMLGenerator xmlGen = new MatViewFallbackQueryXMLGenerator(xmlquery, stmt.m_groupByColumns, stmt.m_displayColumns);
            List<VoltXMLElement> fallbackQueryXMLs = xmlGen.getFallbackQueryXMLs();
            compileFallbackQueriesAndUpdateCatalog(db, query, fallbackQueryXMLs, matviewinfo);
            compileFallbackQueriesAndUpdateCatalog(db, query, fallbackQueryXMLs, mvHandlerInfo);
            if (mvHandlerInfo.getIsjoinedtableview()) {
                compileCreateQueryAndUpdateCatalog(db, query, xmlquery, mvHandlerInfo);
            }

            // set Aggregation Expressions.
            if (hasAggregationExprs) {
                String aggregationExprsJson = null;
                try {
                    aggregationExprsJson = DDLCompiler.convertToJSONArray(aggregationExprs);
                } catch (JSONException e) {
                    throw m_compiler.new VoltCompilerException ("Unexpected error serializing non-column " +
                            "expressions for aggregation expressions: " + e.toString());
                }
                matviewinfo.setAggregationexpressionsjson(aggregationExprsJson);
            }

            // Find index for each min/max aggCol/aggExpr (ENG-6511 and ENG-8512)
            for (Integer i=0; i<minMaxAggs.size(); ++i) {
                Index found = findBestMatchIndexForMatviewMinOrMax(matviewinfo, srcTable, groupbyExprs, minMaxAggs.get(i));
                IndexRef refFound = matviewinfo.getIndexforminmax().add(i.toString());
                if (found != null) {
                    refFound.setName(found.getTypeName());
                } else {
                    refFound.setName("");
                }
            }

            // parse out the aggregation columns into the dest table
            for (int i = stmt.m_groupByColumns.size() + 1; i < stmt.m_displayColumns.size(); i++) {
                ParsedColInfo col = stmt.m_displayColumns.get(i);
                Column destColumn = destColumnArray.get(i);

                AbstractExpression colExpr = col.expression.getLeft();
                TupleValueExpression tve = null;
                if (colExpr.getExpressionType() == ExpressionType.VALUE_TUPLE) {
                    tve = (TupleValueExpression)colExpr;
                }
                processMaterializedViewColumn(matviewinfo, srcTable, destColumn,
                        col.expression.getExpressionType(), tve);

                // Correctly set the type of the column so that it's consistent.
                // Otherwise HSQLDB might promote types differently than Volt.
                destColumn.setType(col.expression.getValueType().getValue());
            }
            if (srcTable.getPartitioncolumn() != null) {
                // Set the partitioning of destination tables of associated views.
                // If a view's source table is replicated, then a full scan of the
                // associated view is single-sited. If the source is partitioned,
                // a full scan of the view must be distributed, unless it is filtered
                // by the original table's partitioning key, which, to be filtered,
                // must also be a GROUP BY key.
                destTable.setIsreplicated(false);
                setGroupedTablePartitionColumn(matviewinfo, srcTable.getPartitioncolumn());
            }
        }
    }

    private void setGroupedTablePartitionColumn(MaterializedViewInfo mvi, Column partitionColumn)
            throws VoltCompilerException {
        // A view of a replicated table is replicated.
        // A view of a partitioned table is partitioned -- regardless of whether it has a partition key
        // -- it certainly isn't replicated!
        // If the partitioning column is grouped, its counterpart is the partitioning column of the view table.
        // Otherwise, the view table just doesn't have a partitioning column
        // -- it is seemingly randomly distributed,
        // and its grouped columns are only locally unique but not globally unique.
        Table destTable = mvi.getDest();
        // Get the grouped columns in "index" order.
        // This order corresponds to the iteration order of the MaterializedViewInfo's group by columns.
        List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(destTable.getColumns(), "index");
        String partitionColName = partitionColumn.getTypeName(); // Note getTypeName gets the column name -- go figure.

        if (mvi.getGroupbycols().size() > 0) {
            int index = 0;
            for (ColumnRef cref : CatalogUtil.getSortedCatalogItems(mvi.getGroupbycols(), "index")) {
                Column srcCol = cref.getColumn();
                if (srcCol.getName().equals(partitionColName)) {
                    Column destCol = destColumnArray.get(index);
                    destTable.setPartitioncolumn(destCol);
                    return;
                }
                ++index;
            }
        } else {
            String complexGroupbyJson = mvi.getGroupbyexpressionsjson();
            if (complexGroupbyJson.length() > 0) {
                int partitionColIndex =  partitionColumn.getIndex();

                  List<AbstractExpression> mvComplexGroupbyCols = null;
                  try {
                      mvComplexGroupbyCols = AbstractExpression.fromJSONArrayString(complexGroupbyJson, null);
                  } catch (JSONException e) {
                      e.printStackTrace();
                  }
                  int index = 0;
                  for (AbstractExpression expr: mvComplexGroupbyCols) {
                      if (expr instanceof TupleValueExpression) {
                          TupleValueExpression tve = (TupleValueExpression) expr;
                          if (tve.getColumnIndex() == partitionColIndex) {
                              Column destCol = destColumnArray.get(index);
                              destTable.setPartitioncolumn(destCol);
                              return;
                          }
                      }
                      ++index;
                  }
            }
        }
    }

    /**
     * If the view is defined on joined tables (>1 source table),
     * check if there are self-joins.
     *
     * @param tableList The list of view source tables.
     * @param compiler The VoltCompiler
     * @throws VoltCompilerException
     */
    private void checkViewSources(ArrayList<Table> tableList) throws VoltCompilerException {
        HashSet<String> tableSet = new HashSet<String>();
        for (Table tbl : tableList) {
            if (! tableSet.add(tbl.getTypeName())) {
                String errMsg = "Table " + tbl.getTypeName() + " appeared in the table list more than once: " +
                                "materialized view does not support self-join.";
                throw m_compiler.new VoltCompilerException(errMsg);
            }
        }
    }

    /**
     * Verify the materialized view meets our arcane rules about what can and can't
     * go in a materialized view. Throw hopefully helpful error messages when these
     * rules are inevitably borked.
     *
     * @param viewName The name of the view being checked.
     * @param stmt The output from the parser describing the select statement that creates the view.
     * @throws VoltCompilerException
     */
    private void checkViewMeetsSpec(String viewName, ParsedSelectStmt stmt) throws VoltCompilerException {
        int groupColCount = stmt.m_groupByColumns.size();
        int displayColCount = stmt.m_displayColumns.size();
        StringBuffer msg = new StringBuffer();
        msg.append("Materialized view \"" + viewName + "\" ");

        List <AbstractExpression> checkExpressions = new ArrayList<AbstractExpression>();

        int i;
        // First, check the group by columns.  They are at
        // the beginning of the display list.
        for (i = 0; i < groupColCount; i++) {
            ParsedColInfo gbcol = stmt.m_groupByColumns.get(i);
            ParsedColInfo outcol = stmt.m_displayColumns.get(i);
            // The columns must be equal.
            if (!outcol.expression.equals(gbcol.expression)) {
                msg.append("must exactly match the GROUP BY clause at index " + String.valueOf(i) + " of SELECT list.");
                throw m_compiler.new VoltCompilerException(msg.toString());
            }
            // check if the expression return type is not unique indexable
            StringBuffer exprMsg = new StringBuffer();
            if (!outcol.expression.isValueTypeUniqueIndexable(exprMsg)) {
                msg.append("with " + exprMsg + " in GROUP BY clause not supported.");
                throw m_compiler.new VoltCompilerException(msg.toString());
            }
            // collect all the expressions and we will check
            // for other gaurds on all of them together
            checkExpressions.add(outcol.expression);
        }

        // check for count star in the display list
        boolean countStarFound = false;
        if (i < displayColCount) {
            AbstractExpression coli = stmt.m_displayColumns.get(i).expression;
            if (coli.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR) {
                countStarFound = true;
            }
        }

        if (countStarFound == false) {
            msg.append("must have count(*) after the GROUP BY columns (if any) but before the aggregate functions (if any).");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        // Finally, the display columns must have aggregate
        // calls.  But these are not any aggregate calls. They
        // must be count(), min(), max() or sum().
        for (i++; i < displayColCount; i++) {
            ParsedColInfo outcol = stmt.m_displayColumns.get(i);
            // Note that this expression does not catch all aggregates.
            // An instance of count(*) here, or avg() would cause the
            // exception.  We just required count(*) above, but a
            // second one would fail.
            if ((outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_COUNT) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_SUM) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_MIN) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_MAX)) {
                msg.append("must have non-group by columns aggregated by sum, count, min or max.");
                throw m_compiler.new VoltCompilerException(msg.toString());
            }
            // Don't push the expression, though.  Push the argument.
            // We will check for aggregate calls and fail, and we don't
            // want to fail on legal aggregate expressions.
            if (outcol.expression.getLeft() != null) {
                checkExpressions.add(outcol.expression.getLeft());
            }
            assert(outcol.expression.getRight() == null);
            assert(outcol.expression.getArgs() == null || outcol.expression.getArgs().size() == 0);
        }

        AbstractExpression where = stmt.getSingleTableFilterExpression();
        if (where != null) {
            checkExpressions.add(where);
        }

        // Check all the subexpressions we gathered up.
        if (!AbstractExpression.validateExprsForIndexesAndMVs(checkExpressions, msg)) {
            // The error message will be in the StringBuffer msg.
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        // Check some other materialized view specific things.
        if (stmt.hasSubquery()) {
            msg.append("with subquery sources is not supported.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        if (! stmt.m_joinTree.allInnerJoins()) {
            throw m_compiler.new VoltCompilerException("Materialized view only supports INNER JOIN.");
        }

        if (stmt.orderByColumns().size() != 0) {
            msg.append("with ORDER BY clause is not supported.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        if (stmt.hasLimitOrOffset()) {
            msg.append("with LIMIT or OFFSET clause is not supported.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        if (stmt.m_having != null) {
            msg.append("with HAVING clause is not supported.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        if (displayColCount <= groupColCount) {
            msg.append("has too few columns.");
            throw m_compiler.new VoltCompilerException(msg.toString());
        }

        checkViewSources(stmt.m_tableList);
     }

    private static void processMaterializedViewColumn(MaterializedViewInfo info, Table srcTable,
            Column destColumn, ExpressionType type, TupleValueExpression colExpr) {

        if (colExpr != null) {
            // assert(colExpr.getTableName().equalsIgnoreCase(srcTable.getTypeName()));
            String srcColName = colExpr.getColumnName();
            Column srcColumn = srcTable.getColumns().getIgnoreCase(srcColName);
            destColumn.setMatviewsource(srcColumn);
        }
        destColumn.setAggregatetype(type.getValue());
    }

    // Compile the fallback query XMLs, add the plans into the catalog statement (ENG-8641).
    private void compileFallbackQueriesAndUpdateCatalog(Database db,
                                                        String query,
                                                        List<VoltXMLElement> fallbackQueryXMLs,
                                                        MaterializedViewInfo matviewinfo) throws VoltCompilerException {
        DatabaseEstimates estimates = new DatabaseEstimates();
        for (int i=0; i<fallbackQueryXMLs.size(); ++i) {
            String key = String.valueOf(i);
            Statement fallbackQueryStmt = matviewinfo.getFallbackquerystmts().add(key);
            VoltXMLElement fallbackQueryXML = fallbackQueryXMLs.get(i);
            fallbackQueryStmt.setSqltext(query);
            StatementCompiler.compileStatementAndUpdateCatalog(m_compiler,
                              m_hsql,
                              db.getCatalog(),
                              db,
                              estimates,
                              fallbackQueryStmt,
                              fallbackQueryXML,
                              fallbackQueryStmt.getSqltext(),
                              null, // no user-supplied join order
                              DeterminismMode.FASTER,
                              StatementPartitioning.forceSP());
        }
    }

    // Compile the fallback query XMLs, add the plans into the catalog statement (ENG-8641).
    private void compileFallbackQueriesAndUpdateCatalog(Database db,
                                                        String query,
                                                        List<VoltXMLElement> fallbackQueryXMLs,
                                                        MaterializedViewHandlerInfo mvHandlerInfo)
                                                        throws VoltCompilerException {
        DatabaseEstimates estimates = new DatabaseEstimates();
        for (int i=0; i<fallbackQueryXMLs.size(); ++i) {
            String key = String.valueOf(i);
            Statement fallbackQueryStmt = mvHandlerInfo.getFallbackquerystmts().add(key);
            VoltXMLElement fallbackQueryXML = fallbackQueryXMLs.get(i);
            fallbackQueryStmt.setSqltext(query);
            StatementCompiler.compileStatementAndUpdateCatalog(m_compiler,
                              m_hsql,
                              db.getCatalog(),
                              db,
                              estimates,
                              fallbackQueryStmt,
                              fallbackQueryXML,
                              fallbackQueryStmt.getSqltext(),
                              null, // no user-supplied join order
                              DeterminismMode.FASTER,
                              StatementPartitioning.forceSP());
        }
    }

    private void compileCreateQueryAndUpdateCatalog(Database db,
                                                    String query,
                                                    VoltXMLElement xmlquery,
                                                    MaterializedViewHandlerInfo mvHandlerInfo)
                                                    throws VoltCompilerException {
        DatabaseEstimates estimates = new DatabaseEstimates();
        Statement createQuery = mvHandlerInfo.getCreatequery().add("createQuery");
        createQuery.setSqltext(query);
        StatementCompiler.compileStatementAndUpdateCatalog(m_compiler,
                          m_hsql,
                          db.getCatalog(),
                          db,
                          estimates,
                          createQuery,
                          xmlquery,
                          createQuery.getSqltext(),
                          null, // no user-supplied join order
                          DeterminismMode.FASTER,
                          StatementPartitioning.inferPartitioning());
    }

    /**
     * Process materialized view warnings.
     */
    public void processMaterializedViewWarnings(Database db, HashMap<Table, String> matViewMap) throws VoltCompilerException {
        HashSet <String> viewTableNames = new HashSet<>();
        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            viewTableNames.add(entry.getKey().getTypeName());
        }

        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            Table destTable = entry.getKey();
            String query = entry.getValue();

            // get the xml for the query
            VoltXMLElement xmlquery = null;
            try {
                xmlquery = m_hsql.getXMLCompiledStatement(query);
            }
            catch (HSQLParseException e) {
                e.printStackTrace();
            }
            assert(xmlquery != null);

            // parse the xml like any other sql statement
            ParsedSelectStmt stmt = null;
            try {
                stmt = (ParsedSelectStmt) AbstractParsedStmt.parse(query, xmlquery, null, db, null);
            }
            catch (Exception e) {
                throw m_compiler.new VoltCompilerException(e.getMessage());
            }
            assert(stmt != null);

            String viewName = destTable.getTypeName();
            // create the materializedviewinfo catalog node for the source table
            Table srcTable = stmt.m_tableList.get(0);
            //export table does not need agg min and max warning.
            if (CatalogUtil.isTableExportOnly(db, srcTable)) continue;

            MaterializedViewInfo matviewinfo = srcTable.getViews().get(viewName);

            boolean hasMinOrMaxAgg = false;
            ArrayList<AbstractExpression> minMaxAggs = new ArrayList<AbstractExpression>();
            for (int i = stmt.m_groupByColumns.size() + 1; i < stmt.m_displayColumns.size(); i++) {
                ParsedColInfo col = stmt.m_displayColumns.get(i);
                AbstractExpression aggExpr = col.expression.getLeft();
                if (col.expression.getExpressionType() ==  ExpressionType.AGGREGATE_MIN ||
                        col.expression.getExpressionType() == ExpressionType.AGGREGATE_MAX) {
                    hasMinOrMaxAgg = true;
                    minMaxAggs.add(aggExpr);
                }
            }

            if (hasMinOrMaxAgg) {
                List<AbstractExpression> groupbyExprs = null;

                if (stmt.hasComplexGroupby()) {
                    groupbyExprs = new ArrayList<AbstractExpression>();
                    for (ParsedColInfo col: stmt.m_groupByColumns) {
                        groupbyExprs.add(col.expression);
                    }
                }

                // Find index for each min/max aggCol/aggExpr (ENG-6511 and ENG-8512)
                boolean needsWarning = false;
                for (Integer i=0; i<minMaxAggs.size(); ++i) {
                    Index found = findBestMatchIndexForMatviewMinOrMax(matviewinfo, srcTable, groupbyExprs, minMaxAggs.get(i));
                    if (found == null) {
                        needsWarning = true;
                    }
                }
                if (needsWarning) {
                    m_compiler.addWarn("No index found to support UPDATE and DELETE on some of the min() / max() columns in the Materialized View " +
                            matviewinfo.getTypeName() +
                            ", and a sequential scan might be issued when current min / max value is updated / deleted.");
                }
            }
        }
    }

    private enum MatViewIndexMatchingGroupby {GB_COL_IDX_COL, GB_COL_IDX_EXP,  GB_EXP_IDX_EXP}

    // if the materialized view has MIN / MAX, try to find an index defined on the source table
    // covering all group by cols / exprs to avoid expensive tablescan.
    // For now, the only acceptable index is defined exactly on the group by columns IN ORDER.
    // This allows the same key to be used to do lookups on the grouped table index and the
    // base table index.
    // TODO: More flexible (but usually less optimal*) indexes may be allowed here and supported
    // in the EE in the future including:
    //   -- *indexes on the group keys listed out of order
    //   -- *indexes on the group keys as a prefix before other indexed values.
    //   -- (ENG-6511) indexes on the group keys PLUS the MIN/MAX argument value (to eliminate post-filtering)
    // This function is mostly re-written for the fix of ENG-6511. --yzhang
    private static Index findBestMatchIndexForMatviewMinOrMax(MaterializedViewInfo matviewinfo,
            Table srcTable, List<AbstractExpression> groupbyExprs, AbstractExpression minMaxAggExpr) {
        CatalogMap<Index> allIndexes = srcTable.getIndexes();
        StmtTableScan tableScan = new StmtTargetTableScan(srcTable, srcTable.getTypeName());

        // Candidate index. If we can find an index covering both group-by columns and aggExpr (optimal) then we will
        // return immediately.
        // If the index found covers only group-by columns (sub-optimal), we will first cache it here.
        Index candidate = null;
        for (Index index : allIndexes) {
            // indexOptimalForMinMax == true if the index covered both the group-by columns and the min/max aggExpr.
            boolean indexOptimalForMinMax = false;
            // If minMaxAggExpr is not null, the diff can be zero or one.
            // Otherwise, for a usable index, its number of columns must agree with that of the group-by columns.
            final int diffAllowance = minMaxAggExpr == null ? 0 : 1;

            // Get all indexed exprs if there is any.
            String expressionjson = index.getExpressionsjson();
            List<AbstractExpression> indexedExprs = null;
            if ( ! expressionjson.isEmpty() ) {
                try {
                    indexedExprs = AbstractExpression.fromJSONArrayString(expressionjson, tableScan);
                } catch (JSONException e) {
                    e.printStackTrace();
                    assert(false);
                    return null;
                }
            }
            // Get source table columns.
            List<Column> srcColumnArray = CatalogUtil.getSortedCatalogItems(srcTable.getColumns(), "index");
            MatViewIndexMatchingGroupby matchingCase = null;

            if (groupbyExprs == null) {
                // This means group-by columns are all simple columns.
                // It also means we can only access the group-by columns by colref.
                List<ColumnRef> groupbyColRefs =
                    CatalogUtil.getSortedCatalogItems(matviewinfo.getGroupbycols(), "index");
                if (indexedExprs == null) {
                    matchingCase = MatViewIndexMatchingGroupby.GB_COL_IDX_COL;

                    // All the columns in the index are also simple columns, EASY! colref vs. colref
                    List<ColumnRef> indexedColRefs =
                        CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
                    // The number of columns in index can never be less than that in the group-by column list.
                    // If minMaxAggExpr == null, they must be equal (diffAllowance == 0)
                    // Otherwise they may be equal (sub-optimal) or
                    // indexedColRefs.size() == groupbyColRefs.size() + 1 (optimal, diffAllowance == 1)
                    if (isInvalidIndexCandidate(indexedColRefs.size(), groupbyColRefs.size(), diffAllowance)) {
                        continue;
                    }

                    if (! isGroupbyMatchingIndex(matchingCase, groupbyColRefs, null, indexedColRefs, null, null)) {
                        continue;
                    }
                    if (isValidIndexCandidateForMinMax(indexedColRefs.size(), groupbyColRefs.size(), diffAllowance)) {
                        if(! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, indexedColRefs, null, srcColumnArray)) {
                            continue;
                        }
                        indexOptimalForMinMax = true;
                    }
                }
                else {
                    matchingCase = MatViewIndexMatchingGroupby.GB_COL_IDX_EXP;
                    // In this branch, group-by columns are simple columns, but the index contains complex columns.
                    // So it's only safe to access the index columns from indexedExprs.
                    // You can still get something from indexedColRefs, but they will be inaccurate.
                    // e.g.: ONE index column (a+b) will get you TWO separate entries {a, b} in indexedColRefs.
                    // In order to compare columns: for group-by columns: convert colref => col
                    //                              for    index columns: convert    tve => col
                    if (isInvalidIndexCandidate(indexedExprs.size(), groupbyColRefs.size(), diffAllowance)) {
                        continue;
                    }

                    if (! isGroupbyMatchingIndex(matchingCase, groupbyColRefs, null, null, indexedExprs, srcColumnArray)) {
                        continue;
                    }
                    if (isValidIndexCandidateForMinMax(indexedExprs.size(), groupbyColRefs.size(), diffAllowance)) {
                        if(! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, null, indexedExprs, null)) {
                            continue;
                        }
                        indexOptimalForMinMax = true;
                    }
                }
            }
            else {
                matchingCase = MatViewIndexMatchingGroupby.GB_EXP_IDX_EXP;
                // This means group-by columns have complex columns.
                // It's only safe to access the group-by columns from groupbyExprs.
                // AND, indexedExprs must not be null in this case. (yeah!)
                if ( indexedExprs == null ) {
                    continue;
                }
                if (isInvalidIndexCandidate(indexedExprs.size(), groupbyExprs.size(), diffAllowance)) {
                    continue;
                }

                if (! isGroupbyMatchingIndex(matchingCase, null, groupbyExprs, null, indexedExprs, null)) {
                    continue;
                }

                if (isValidIndexCandidateForMinMax(indexedExprs.size(), groupbyExprs.size(), diffAllowance)) {
                    if (! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, null, indexedExprs, null)) {
                        continue;
                    }
                    indexOptimalForMinMax = true;
                }
            }

            // NOW index at least covered all group-by columns (sub-optimal candidate)
            if (!index.getPredicatejson().isEmpty()) {
                // Additional check for partial indexes to make sure matview WHERE clause
                // covers the partial index predicate
                List<AbstractExpression> coveringExprs = new ArrayList<AbstractExpression>();
                List<AbstractExpression> exactMatchCoveringExprs = new ArrayList<AbstractExpression>();
                try {
                    String encodedPredicate = matviewinfo.getPredicate();
                    if (!encodedPredicate.isEmpty()) {
                        String predicate = Encoder.hexDecodeToString(encodedPredicate);
                        AbstractExpression matViewPredicate = AbstractExpression.fromJSONString(predicate, tableScan);
                        coveringExprs.addAll(ExpressionUtil.uncombineAny(matViewPredicate));
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    assert(false);
                    return null;
                }
                if (! SubPlanAssembler.isPartialIndexPredicateCovered(tableScan, coveringExprs, index, exactMatchCoveringExprs)) {
                    // partial index does not match MatView where clause, give up this index
                    continue;
                }
            }
            // if the index already covered group by columns and the aggCol/aggExpr,
            // it is already the best index we can get, return immediately.
            if (indexOptimalForMinMax) {
                return index;
            }
            // otherwise wait to see if we can find something better!
            candidate = index;
        }
        return candidate;
    }

    private static boolean isInvalidIndexCandidate(int idxSize, int gbSize, int diffAllowance) {
        if ( idxSize < gbSize || idxSize > gbSize + diffAllowance ) {
            return true;
        }
        return false;
    }

    private static boolean isGroupbyMatchingIndex(
            MatViewIndexMatchingGroupby matchingCase,
            List<ColumnRef> groupbyColRefs, List<AbstractExpression> groupbyExprs,
            List<ColumnRef> indexedColRefs, List<AbstractExpression> indexedExprs,
            List<Column> srcColumnArray) {
        // Compare group-by columns/expressions for different cases
        switch(matchingCase) {
        case GB_COL_IDX_COL:
            for (int i = 0; i < groupbyColRefs.size(); ++i) {
                int groupbyColIndex = groupbyColRefs.get(i).getColumn().getIndex();
                int indexedColIndex = indexedColRefs.get(i).getColumn().getIndex();
                if (groupbyColIndex != indexedColIndex) {
                    return false;
                }
            }
            break;
        case GB_COL_IDX_EXP:
            for (int i = 0; i < groupbyColRefs.size(); ++i) {
                AbstractExpression indexedExpr = indexedExprs.get(i);
                if (! (indexedExpr instanceof TupleValueExpression)) {
                    // Group-by columns are all simple columns, so indexedExpr must be tve.
                    return false;
                }
                int indexedColIdx = ((TupleValueExpression)indexedExpr).getColumnIndex();
                Column indexedColumn = srcColumnArray.get(indexedColIdx);
                Column groupbyColumn = groupbyColRefs.get(i).getColumn();
                if ( ! indexedColumn.equals(groupbyColumn) ) {
                    return false;
                }
            }
            break;
        case GB_EXP_IDX_EXP:
            for (int i = 0; i < groupbyExprs.size(); ++i) {
                if (! indexedExprs.get(i).equals(groupbyExprs.get(i))) {
                   return false;
                }
            }
            break;
        default:
            assert(false);
            // invalid option
            return false;
        }

        // group-by columns/expressions are matched with the corresponding index
        return true;
    }

    private static boolean isValidIndexCandidateForMinMax(int idxSize, int gbSize, int diffAllowance) {
        return diffAllowance == 1 && idxSize == gbSize + 1;
    }

    private static boolean isIndexOptimalForMinMax(
            MatViewIndexMatchingGroupby matchingCase, AbstractExpression minMaxAggExpr,
            List<ColumnRef> indexedColRefs, List<AbstractExpression> indexedExprs,
            List<Column> srcColumnArray) {
        // We have minMaxAggExpr and the index also has one extra column
        switch(matchingCase) {
        case GB_COL_IDX_COL:
            if ( ! (minMaxAggExpr instanceof TupleValueExpression) ) {
                // Here because the index columns are all simple columns (indexedExprs == null)
                // so the minMaxAggExpr must be TupleValueExpression.
                return false;
            }
            int aggSrcColIdx = ((TupleValueExpression)minMaxAggExpr).getColumnIndex();
            Column aggSrcCol = srcColumnArray.get(aggSrcColIdx);
            Column lastIndexCol = indexedColRefs.get(indexedColRefs.size() - 1).getColumn();
            // Compare the two columns, if they are equal as well, then this is the optimal index! Congrats!
            if (aggSrcCol.equals(lastIndexCol)) {
                return true;
            }
            break;
        case GB_COL_IDX_EXP:
        case GB_EXP_IDX_EXP:
            if (indexedExprs.get(indexedExprs.size()-1).equals(minMaxAggExpr)) {
                return true;
            }
            break;
        default:
            assert(false);
        }

        // If the last part of the index does not match the MIN/MAX expression
        // this is not the optimal index candidate for now
        return false;
    }
}
