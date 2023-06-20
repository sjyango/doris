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

#include "vec/exec/join/vsort_merge_join_node.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <string.h>

#include <boost/iterator/iterator_facade.hpp>
#include <functional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exec/exec_node.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_slots_cross.h"
#include "gutil/integral_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/simd/bits.h"
#include "util/telemetry/telemetry.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

VSortMergeJoinNode::VSortMergeJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs)
        : VJoinNodeBase(pool, tnode, descs),
          _runtime_filter_descs(tnode.runtime_filters) {}

Status VSortMergeJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VJoinNodeBase::init(tnode, state));
    DCHECK_EQ(children_count(), 2);

    // if (tnode.sort_merge_join_node.__isset.is_output_left_side_only) {
    //    _is_output_left_side_only = tnode.sort_merge_join_node.is_output_left_side_only;
    // }

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.sort_merge_join_node.eq_join_conjuncts;
    std::vector<bool> probe_not_ignore_null(eq_join_conjuncts.size());

    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        _left_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _right_expr_ctxs.push_back(ctx);
    }

    if (tnode.sort_merge_join_node.__isset.other_join_conjuncts &&
        !tnode.sort_merge_join_node.other_join_conjuncts.empty()) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.sort_merge_join_node.other_join_conjuncts,
                                                 _other_join_conjuncts));
        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    }

    std::vector<TExpr> filter_src_exprs;

    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        filter_src_exprs.push_back(_runtime_filter_descs[i].src_expr);
        RETURN_IF_ERROR(state->runtime_filter_mgr()->register_filter(
                RuntimeFilterRole::PRODUCER, _runtime_filter_descs[i], state->query_options()));
    }

    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(filter_src_exprs, _filter_src_expr_ctxs));

    return Status::OK();
}

Status VSortMergeJoinNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::prepare(state));
    DCHECK_EQ(children_count(), 2);

    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _build_rows_counter = ADD_COUNTER(runtime_profile(), "BuildRows", TUnit::UNIT);
    _probe_rows_counter = ADD_COUNTER(runtime_profile(), "ProbeRows", TUnit::UNIT);
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");
    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    _join_filter_timer = ADD_TIMER(runtime_profile(), "JoinFilterTimer");

    RETURN_IF_ERROR(VExpr::prepare(_left_expr_ctxs, state, child(0)->row_desc()));
    RETURN_IF_ERROR(VExpr::prepare(_right_expr_ctxs, state, child(1)->row_desc()));

    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
    }

    _num_left_columns = child(0)->row_desc().num_materialized_slots();
    _num_right_columns = child(1)->row_desc().num_materialized_slots();
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    RETURN_IF_ERROR(VExpr::prepare(_filter_src_expr_ctxs, state, child(1)->row_desc()));

    // build _join_block
    _construct_mutable_join_block();
    // build _left_join_columns and _right_join_columns
    _construct_mutable_join_columns();

    _left_cursor = std::make_unique<MergeJoinCursor>(static_cast<VSortNode*>(child(0)), &_join_block, state);
    _right_cursor = std::make_unique<MergeJoinCursor>(static_cast<VSortNode*>(child(1)), &_join_block, state);

    return Status::OK();
}

Status VSortMergeJoinNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_left_cursor->fetch_next_block());
    RETURN_IF_ERROR(_right_cursor->fetch_next_block());
    _left_cursor->set_compare_nullability();
    _right_cursor->set_compare_nullability();
    return Status::OK();
}

Status VSortMergeJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    _release_mem();

    Status s = VJoinNodeBase::close(state);
    child(0)->close(state);
    child(1)->close(state);
    return s;
}

void VSortMergeJoinNode::_release_mem() {
    _left_join_columns.clear();
    _right_join_columns.clear();

    _left_cursor.reset();
    _right_cursor.reset();

    _tuple_is_null_left_flag_column = nullptr;
    _tuple_is_null_right_flag_column = nullptr;
}

Status VSortMergeJoinNode::_materialize_build_side(RuntimeState* state) {
    RETURN_IF_ERROR(child(1)->open(state));
    return Status::OK();
}

void VSortMergeJoinNode::_add_tuple_is_null_column(Block* block) {}

Status VSortMergeJoinNode::sink(doris::RuntimeState* state, vectorized::Block* block, bool eos) {
    SCOPED_TIMER(_build_timer);

    return Status::OK();
}

Status VSortMergeJoinNode::push(doris::RuntimeState* state, vectorized::Block* block, bool eos) {
    // COUNTER_UPDATE(_probe_rows_counter, block->rows());

    RETURN_IF_ERROR(_do_sort_merge_join());

    return Status::OK();
}

Status VSortMergeJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);
    RETURN_IF_CANCELLED(state);

    if (can_push_more_data()) {
        push(state, &_join_block, false);
    }

    return pull(state, block, eos);
}

Status VSortMergeJoinNode::pull(RuntimeState* state, vectorized::Block* block, bool* eos) {
    *eos = !can_push_more_data();
    _construct_result_join_block();
    {
        SCOPED_TIMER(_join_filter_timer);
        RETURN_IF_ERROR(
                VExprContext::filter_block(_conjuncts, &_join_block, _join_block.columns()));
    }
    RETURN_IF_ERROR(_build_output_block(&_join_block, block));
    _reset_tuple_is_null_column();
    _join_block.clear_column_data();
    _construct_mutable_join_columns();
    reached_limit(block, eos);
    return Status::OK();
}

Status VSortMergeJoinNode::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(VJoinNodeBase::alloc_resource(state));
    RETURN_IF_ERROR(VExpr::open(_left_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_right_expr_ctxs, state));

    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }

    return VExpr::open(_filter_src_expr_ctxs, state);
}

void VSortMergeJoinNode::release_resource(doris::RuntimeState* state) {
    VExpr::close(_left_expr_ctxs, state);
    VExpr::close(_right_expr_ctxs, state);

    for (auto& conjunct : _other_join_conjuncts) {
        conjunct->close(state);
    }

    _release_mem();
    VJoinNodeBase::release_resource(state);
}

void VSortMergeJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << "VSortMergeJoinNode";
    *out << "(can_push_more_data=" << (can_push_more_data() ? "true" : "false");
    *out << ")\n children=";
    VJoinNodeBase::debug_string(indentation_level, out);
    *out << ")";
}

bool VSortMergeJoinNode::can_push_more_data() const {
    DCHECK_EQ(_left_join_columns[0]->size(), _right_join_columns[0]->size());
    return !_left_cursor->at_end() && !_right_cursor->at_end()
           && _left_join_columns[0]->size() < _SORT_MERGE_JOIN_BLOCK_SIZE_THRESHOLD;
}

} // namespace doris::vectorized