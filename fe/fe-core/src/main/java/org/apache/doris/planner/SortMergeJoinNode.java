// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BitmapFilterPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TEqJoinCondition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TSortMergeJoinNode;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sort merge join between left child and right child.
 */
public class SortMergeJoinNode extends JoinNodeBase {
    private static final Logger LOG = LogManager.getLogger(SortMergeJoinNode.class);

    // predicates of the form 'a=b' or 'a<=>b'
    private List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();
    // join conjuncts from the JOIN clause that aren't equi-join predicates
    private List<Expr> otherJoinConjuncts;

    private List<Expr> runtimeFilterExpr = Lists.newArrayList();

    private PlanNodeId leftSortNodeId;
    private PlanNodeId rightSortNodeId;

    public SortMergeJoinNode(PlanNodeId id, PlanNodeId leftSortNodeId, PlanNodeId rightSortNodeId,
                             PlanNode leftNode, PlanNode rightNode, TableRef innerRef,
                             List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts) {
        super(id, "SORT MERGE JOIN", StatisticalType.SORT_MERGE_JOIN_NODE, leftNode, rightNode, innerRef);
        Preconditions.checkArgument(eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty());
        Preconditions.checkArgument(otherJoinConjuncts != null);

        tupleIds.addAll(leftNode.getTupleIds());
        tupleIds.addAll(rightNode.getTupleIds());
        this.leftSortNodeId = leftSortNodeId;
        this.rightSortNodeId = rightSortNodeId;

        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            Preconditions.checkArgument(eqJoinPredicate instanceof BinaryPredicate);
            BinaryPredicate eqJoin = (BinaryPredicate) eqJoinPredicate;
            if (eqJoin.getOp().equals(BinaryPredicate.Operator.EQ_FOR_NULL)) {
                Preconditions.checkArgument(eqJoin.getChildren().size() == 2);
                if (!eqJoin.getChild(0).isNullable() || !eqJoin.getChild(1).isNullable()) {
                    eqJoin.setOp(BinaryPredicate.Operator.EQ);
                }
            }
            this.eqJoinConjuncts.add(eqJoin);
        }

        this.otherJoinConjuncts = otherJoinConjuncts;
    }

    public boolean canParallelize() {
        return joinOp == JoinOperator.CROSS_JOIN || joinOp == JoinOperator.INNER_JOIN
                || joinOp == JoinOperator.LEFT_OUTER_JOIN || joinOp == JoinOperator.LEFT_SEMI_JOIN
                || joinOp == JoinOperator.LEFT_ANTI_JOIN || joinOp == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    @Override
    protected List<SlotId> computeSlotIdsForJoinConjuncts(Analyzer analyzer) {
        // eq conjunct
        List<SlotId> joinConjunctSlotIds = Lists.newArrayList();
        Expr.getIds(eqJoinConjuncts, null, joinConjunctSlotIds);
        // other conjunct
        List<SlotId> otherConjunctSlotIds = Lists.newArrayList();
        Expr.getIds(otherJoinConjuncts, null, otherConjunctSlotIds);
        joinConjunctSlotIds.addAll(otherConjunctSlotIds);
        return joinConjunctSlotIds;
    }

    @Override
    protected Pair<Boolean, Boolean> needToCopyRightAndLeft() {
        boolean copyleft = true;
        boolean copyRight = true;
        return Pair.of(copyleft, copyRight);
    }

    /**
     * Only for Nereids.
     */
    public SortMergeJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, List<TupleId> tupleIds,
                             JoinOperator joinOperator, List<Expr> srcToOutputList, TupleDescriptor intermediateTuple,
                             TupleDescriptor outputTuple, boolean isMarkJoin) {
        super(id, "SORT MERGE JOIN", StatisticalType.SORT_MERGE_JOIN_NODE, joinOperator, isMarkJoin);
        this.tupleIds.addAll(tupleIds);
        children.add(outer);
        children.add(inner);
        // TODO: need to set joinOp by Nereids

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
        }
        vIntermediateTupleDescList = Lists.newArrayList(intermediateTuple);
        vOutputTupleDesc = outputTuple;
        vSrcToOutputSMap = new ExprSubstitutionMap(srcToOutputList, Collections.emptyList());
    }

    public List<Expr> getRuntimeFilterExpr() {
        return runtimeFilterExpr;
    }

    public void addBitmapFilterExpr(Expr runtimeFilterExpr) {
        this.runtimeFilterExpr.add(runtimeFilterExpr);
    }

    public TableRef getInnerRef() {
        return innerRef;
    }

    @Override
    protected void computeOldCardinality() {
        if (getChild(0).cardinality == -1 || getChild(1).cardinality == -1) {
            cardinality = -1;
        } else {
            cardinality = getChild(0).cardinality * getChild(1).cardinality;
            if (computeOldSelectivity() != -1) {
                cardinality = Math.round(((double) cardinality) * computeOldSelectivity());
            }
        }
        LOG.debug("stats SortMergeJoin: cardinality={}", Long.toString(cardinality));
    }

    @Override
    protected void computeOtherConjuncts(Analyzer analyzer, ExprSubstitutionMap originToIntermediateSmap) {
        otherJoinConjuncts = Expr.substituteList(otherJoinConjuncts, originToIntermediateSmap, analyzer, false);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).addValue(super.debugString()).toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.sort_merge_join_node = new TSortMergeJoinNode();
        msg.sort_merge_join_node.join_op = joinOp.toThrift();
        msg.sort_merge_join_node.setIsMark(isMarkJoin());

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                    eqJoinPredicate.getChild(1).treeToThrift());
            eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
            msg.sort_merge_join_node.addToEqJoinConjuncts(eqJoinCondition);
        }
        for (Expr e : otherJoinConjuncts) {
            msg.sort_merge_join_node.addToOtherJoinConjuncts(e.treeToThrift());
        }

        if (vSrcToOutputSMap != null) {
            for (int i = 0; i < vSrcToOutputSMap.size(); i++) {
                // TODO: Enable it after we support new optimizers
                // if (ConnectContext.get().getSessionVariable().isEnableNereidsPlanner()) {
                //     msg.addToProjections(vSrcToOutputSMap.getLhs().get(i).treeToThrift());
                // } else
                msg.sort_merge_join_node.addToSrcExprList(vSrcToOutputSMap.getLhs().get(i).treeToThrift());
            }
        }
        if (vOutputTupleDesc != null) {
            msg.sort_merge_join_node.setVoutputTupleId(vOutputTupleDesc.getId().asInt());
            // TODO Enable it after we support new optimizers
            // msg.setOutputTupleId(vOutputTupleDesc.getId().asInt());
        }
        if (vIntermediateTupleDescList != null) {
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                msg.sort_merge_join_node.addToVintermediateTupleIdList(tupleDescriptor.getId().asInt());
            }
        }
        // msg.sort_merge_join_node.setIsOutputLeftSideOnly(isOutputLeftSideOnly);
        msg.node_type = TPlanNodeType.SORT_MERGE_JOIN_NODE;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        ExprSubstitutionMap combinedChildSmap = getCombinedChildWithoutTupleIsNullSmap();
        List<Expr> newEqJoinConjuncts = Expr.substituteList(eqJoinConjuncts, combinedChildSmap, analyzer, false);
        eqJoinConjuncts =
            newEqJoinConjuncts.stream().map(entity -> (BinaryPredicate) entity).collect(Collectors.toList());
        otherJoinConjuncts = Expr.substituteList(otherJoinConjuncts, combinedChildSmap, analyzer, false);

        computeCrossRuntimeFilterExpr();

        // insert `sort node` between `sort merge join node` and `child node` if `child node` is not sorted
        if (!(children.get(0) instanceof SortMergeJoinNode) && !(children.get(0) instanceof SortNode)) {
            children.set(0, createSortNode(leftSortNodeId, children.get(0), true, analyzer));
        }

        if (!(children.get(1) instanceof SortMergeJoinNode) && !(children.get(1) instanceof SortNode)) {
            children.set(1, createSortNode(rightSortNodeId, children.get(1), false, analyzer));
        }

        // Only for Vec: create new tuple for join result
        if (VectorizedUtil.isVectorized()) {
            computeOutputTuple(analyzer);
        }
    }

    private SortInfo createSortInfo(PlanNode childNode) {
        Preconditions.checkArgument(!eqJoinConjuncts.isEmpty());
        List<Expr> orderingExprs = Lists.newArrayList();
        List<Boolean> isAscOrder = Lists.newArrayList();
        List<Boolean> nullsFirstParams = Lists.newArrayList();
        ArrayList<TupleId> tupleIds = childNode.getTupleIds();

        for (BinaryPredicate joinConjunct : eqJoinConjuncts) {
            SlotRef left = (SlotRef) joinConjunct.getChild(0);
            SlotRef right = (SlotRef) joinConjunct.getChild(1);

            for (TupleId tupleId : tupleIds) {
                if (tupleId.equals(left.getDesc().getParent().getId())) {
                    orderingExprs.add(left);
                } else if (tupleId.equals(right.getDesc().getParent().getId())) {
                    orderingExprs.add(right);
                }
            }

            isAscOrder.add(true);
            nullsFirstParams.add(true);
        }

        return new SortInfo(orderingExprs, isAscOrder, nullsFirstParams);
    }

    private SortNode createSortNode(PlanNodeId sortNodeId, PlanNode childNode, boolean isLeftSort, Analyzer analyzer)
            throws UserException {
        List<Expr> scanNodeResultExprs = Lists.newArrayList();
        TupleDescriptor tupleDescriptor = null;

        if (childNode instanceof ScanNode) {
            tupleDescriptor = ((ScanNode) childNode).getTupleDesc();
        } else if (childNode instanceof JoinNodeBase) {
            tupleDescriptor = ((JoinNodeBase) childNode).vOutputTupleDesc;
        }

        Preconditions.checkState(tupleDescriptor != null);

        for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
            scanNodeResultExprs.add(new SlotRef(slotDescriptor));
        }

        SortInfo sortInfo = createSortInfo(childNode);

        if (childNode instanceof ScanNode) {
            sortInfo.setSortTupleDesc(((ScanNode) childNode).getTupleDesc());
        } else if (childNode instanceof JoinNodeBase) {
            sortInfo.setSortTupleDesc(((JoinNodeBase) childNode).vOutputTupleDesc);
        }

        sortInfo.setSortTupleSlotExprs(scanNodeResultExprs);

        SortNode sortNode = new SortNode(sortNodeId, childNode, sortInfo, false);
        sortNode.init(analyzer);
        sortNode.setDefaultLimit(true);
        sortNode.setLimit(-1);
        sortNode.setMergeSort(true);
        sortNode.setLeftSort(isLeftSort);
        return sortNode;
    }

    private void computeCrossRuntimeFilterExpr() {
        for (int i = conjuncts.size() - 1; i >= 0; --i) {
            if (conjuncts.get(i) instanceof BitmapFilterPredicate) {
                addBitmapFilterExpr(conjuncts.get(i));
                conjuncts.remove(i);
            }
        }
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr = "";
        StringBuilder output =
                new StringBuilder().append(detailPrefix).append("join op: ").append(joinOp.toString()).append("(")
                        .append(distrModeStr).append(")\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            output.append(detailPrefix).append(
                    String.format("cardinality=%,d", cardinality)).append("\n");
            return output.toString();
        }

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            output.append(detailPrefix).append("equal join conjunct: ").append(eqJoinPredicate.toSql()).append("\n");
        }

        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("other join predicates: ")
                .append(getExplainString(otherJoinConjuncts)).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("other predicates: ").append(getExplainString(conjuncts)).append("\n");
        }

        if (!runtimeFilters.isEmpty()) {
            output.append(detailPrefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(true));
        }
        // output.append(detailPrefix).append("is output left side only: ").append(isOutputLeftSideOnly).append("\n");
        output.append(detailPrefix).append(String.format("cardinality=%,d", cardinality)).append("\n");
        // todo unify in plan node
        if (vOutputTupleDesc != null) {
            output.append(detailPrefix).append("vec output tuple id: ").append(vOutputTupleDesc.getId()).append("\n");
        }
        if (vIntermediateTupleDescList != null) {
            output.append(detailPrefix).append("vIntermediate tuple ids: ");
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                output.append(tupleDescriptor.getId()).append(" ");
            }
            output.append("\n");
        }
        if (outputSlotIds != null) {
            output.append(detailPrefix).append("output slot ids: ");
            for (SlotId slotId : outputSlotIds) {
                output.append(slotId).append(" ");
            }
            output.append("\n");
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            output.append(detailPrefix).append("isMarkJoin: ").append(isMarkJoin()).append("\n");
        }
        return output.toString();
    }
}
