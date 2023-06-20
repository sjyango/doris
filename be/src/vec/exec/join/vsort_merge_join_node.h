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

#pragma once

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <stack>
#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/sort_description.h"
#include "vec/exec/join/vjoin_node_base.h"
#include "vec/exec/vsort_node.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class RowDescriptor;

namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class StreamBlockCursor {
public:
    StreamBlockCursor() = default;

    StreamBlockCursor(VSortNode* node, Block* join_block, RuntimeState* state)
            : _node(node), _state(state) {
        _first_block = Block::create_unique();
        _second_block = Block::create_unique();

        _desc.resize(node->get_sort_exec_exprs().lhs_ordering_expr_ctxs().size());
        for (int i = 0; i < _desc.size(); i++) {
            const auto& ordering_expr = node->get_sort_exec_exprs().lhs_ordering_expr_ctxs()[i];
            auto status = ordering_expr->execute(join_block, &_desc[i].column_number);

            _desc[i].direction = node->get_is_asc_order()[i] ? 1 : -1;
            _desc[i].nulls_direction =
                    node->get_nulls_first()[i] ? -_desc[i].direction : _desc[i].direction;
        }
    }

    size_t sort_columns_size() const { return _desc.size(); }
    size_t position() const { return _cur_pos; }
    size_t& position() { return _cur_pos; }
    size_t end() const { return _max_pos; }
    size_t remaining() const { return _max_pos - _cur_pos; }
    void set_cur_position(size_t pos) { _cur_pos = pos; }
    bool is_valid() const { return _cur_pos < _max_pos; }
    bool at_end() const { return _cur_pos >= _max_pos && _eos; }
    bool is_eos() const { return _eos; }

    Status next() {
        RETURN_IF_ERROR(next(1));
        return Status::OK();
    }

    Status next(size_t size) {
        if (_cur_pos + size >= _max_pos && !_eos) {
            RETURN_IF_ERROR(fetch_next_block());
        }
        _cur_pos += size;
        return Status::OK();
    }

    Status fetch_next_block() {
        DCHECK(!_eos);
        RETURN_IF_CANCELLED(_state);

        // discard first block
        _first_block.swap(_second_block);
        _second_block = Block::create_unique();

        RETURN_IF_ERROR(
                _node->get_next_after_projects(
                        _state, _second_block.get(), &_eos,
                        std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                          ExecNode::get_next,
                                  _node, std::placeholders::_1, std::placeholders::_2,
                                  std::placeholders::_3)));

        _update_all_columns();
        _update_sort_columns();

        _min_pos = _mid_pos;
        _mid_pos = _max_pos;
        _max_pos = _mid_pos + _second_block->rows();

        return Status::OK();
    }

protected:
    const IColumn* _get_cur_sort_column(size_t i) const {
        if (_cur_pos < _mid_pos) {
            return _first_sort_columns[i];
        } else {
            return _second_sort_columns[i];
        }
    }

    void _update_all_columns() {
        _first_all_columns.clear();
        _second_all_columns.clear();

        for (const auto& column : _first_block->get_columns()) {
            _first_all_columns.push_back(column.get());
        }

        for (const auto& column : _second_block->get_columns()) {
            _second_all_columns.push_back(column.get());
        }
    }

    void _update_sort_columns() {
        _first_sort_columns.clear();
        _second_sort_columns.clear();

        auto first_columns = _first_block->get_columns();
        auto second_columns = _second_block->get_columns();

        if (first_columns.size() == _desc.size()) {
            for (size_t i = 0, size = _desc.size(); i < size; ++i) {
                _first_sort_columns.push_back(first_columns[_desc[i].column_number].get());
            }
        }

        for (size_t i = 0, size = _desc.size(); i < size; ++i) {
            _second_sort_columns.push_back(second_columns[_desc[i].column_number].get());
        }
    }

    VSortNode* _node;
    RuntimeState* _state;

    std::unique_ptr<Block> _first_block;
    std::unique_ptr<Block> _second_block;

    ColumnRawPtrs _first_sort_columns;
    ColumnRawPtrs _first_all_columns;
    ColumnRawPtrs _second_sort_columns;
    ColumnRawPtrs _second_all_columns;
    SortDescription _desc;
    size_t _cur_pos = 0;
    size_t _min_pos = 0;
    size_t _mid_pos = 0;
    size_t _max_pos = 0;
    bool _eos;
};

struct MergeJoinEqualRange {
    size_t _left_start = 0;
    size_t _left_length = 0;
    size_t _right_start = 0;
    size_t _right_length = 0;

    bool is_empty() const { return !_left_length && !_right_length; }
};

using Range = MergeJoinEqualRange;

class MergeJoinCursor : public StreamBlockCursor {
public:
    MergeJoinCursor(VSortNode* node, Block* join_block, RuntimeState* state)
            : StreamBlockCursor(node, join_block, state) {}

    void next_n(size_t num) { next(num); }

    bool has_nullable_column() const {
        return _has_nullable_column;
    }

    Range get_next_equal_range(MergeJoinCursor& rhs) {
        if (_has_nullable_column && rhs._has_nullable_column)
            return _get_next_equal_range_impl<true, true>(rhs);
        else if (_has_nullable_column)
            return _get_next_equal_range_impl<true, false>(rhs);
        else if (rhs._has_nullable_column)
            return _get_next_equal_range_impl<false, true>(rhs);
        return _get_next_equal_range_impl<false, false>(rhs);
    }

    size_t get_equal_range_length() {
        DCHECK(!at_end());
        size_t prev_pos = position();
        size_t length = 1;

        while (!at_end()) {
            next();
            if (!_is_same_prev(position())) break;
            ++length;
        }

        set_cur_position(prev_pos);
        return length;
    }

    // get slice [start, start + length)
    Block get_block_from_range(size_t start, size_t length) {
        DCHECK(start >= _min_pos && start + length <= _max_pos);
        Block range_block = _second_block->clone_empty();
        MutableColumns range_block_columns = range_block.mutate_columns();

        if (start + length <= _mid_pos) {
            // slice the first block
            for (size_t i = 0; i < _first_block->columns(); ++i) {
                const auto & src_column = _first_block->get_by_position(i).column;
                range_block_columns[i]->insert_range_from(*src_column, start - _min_pos, length);
            }
        } else if (start < _mid_pos && start + length > _mid_pos) {
            // slice the first block and the second block
            for (size_t i = 0; i < _first_block->columns(); ++i) {
                const auto & src_column = _first_block->get_by_position(i).column;
                range_block_columns[i]->insert_range_from(*src_column, start - _min_pos, _mid_pos - start);
            }

            for (size_t i = 0; i < _second_block->columns(); ++i) {
                const auto & src_column = _second_block->get_by_position(i).column;
                range_block_columns[i]->insert_range_from(*src_column, _mid_pos, start + length - _mid_pos);
            }
        } else if (start >= _mid_pos) {
            for (size_t i = 0; i < _second_block->columns(); ++i) {
                const auto & src_column = _second_block->get_by_position(i).column;
                range_block_columns[i]->insert_range_from(*src_column, start - _mid_pos, length);
            }
        }

        range_block.set_columns(std::move(range_block_columns));
        DCHECK_EQ(range_block.rows(), length);
        return range_block;
    }

    // (row in block, offset)
    std::pair<const Block*, size_t> get_cur_row(size_t pos) const {
        if (pos >= _min_pos && pos < _mid_pos) {
            return std::make_pair(_first_block.get(), pos - _min_pos);
        } else if (pos >= _mid_pos && pos < _max_pos) {
            return std::make_pair(_second_block.get(), pos - _mid_pos);
        }
        return std::make_pair(_second_block.get(), pos - _mid_pos);
    }

    template <bool left_nullable, bool right_nullable>
    static int nullable_compare_at(const IColumn& left_column, const IColumn& right_column, size_t lhs_pos, size_t rhs_pos) {
        static constexpr int null_direction_hint = 1;

        if constexpr (left_nullable && right_nullable) {
            const auto * left_nullable_column = check_and_get_column<ColumnNullable>(left_column);
            const auto * right_nullable_column = check_and_get_column<ColumnNullable>(right_column);

            if (left_nullable_column && right_nullable_column) {
                int res = left_column.compare_at(lhs_pos, rhs_pos, right_column, null_direction_hint);
                if (res) return res;
                /// NULL != NULL case
                if (left_column.is_null_at(lhs_pos)) return null_direction_hint;
                return 0;
            }
        } else if constexpr (left_nullable) {
            if (const auto * left_nullable_column = check_and_get_column<ColumnNullable>(left_column)) {
                if (left_column.is_null_at(lhs_pos))
                    return null_direction_hint;
                return left_nullable_column->get_nested_column()
                        .compare_at(lhs_pos, rhs_pos, right_column, null_direction_hint);
            }
        } else if constexpr (right_nullable) {
            if (const auto * right_nullable_column = check_and_get_column<ColumnNullable>(right_column)) {
                if (right_column.is_null_at(rhs_pos))
                    return -null_direction_hint;
                return left_column.compare_at(lhs_pos, rhs_pos, right_nullable_column->get_nested_column(), null_direction_hint);
            }
        }

        return left_column.compare_at(lhs_pos, rhs_pos, right_column, null_direction_hint);
    }

    void set_compare_nullability() {
        for (size_t i = 0; i < sort_columns_size(); ++i) {
            _has_nullable_column = _has_nullable_column || _is_sort_column_nullable(i);
        }
    }

private:
    bool _is_sort_column_nullable(size_t i) const {
        if (_first_sort_columns.size() == _desc.size()) {
            return is_column_nullable(*_first_sort_columns[i]) || is_column_nullable(*_second_sort_columns[i]);
        }
        return false;
    }

    template <bool left_nullable, bool right_nullable>
    Range _get_next_equal_range_impl(MergeJoinCursor& rhs) {
        size_t left_prev_pos = position();
        size_t right_prev_pos = rhs.position();

        while (!at_end() && !rhs.at_end()) {
            int cmp = _compare_at_cursor<left_nullable, right_nullable>(rhs);
            if (cmp < 0) {
                next();
            } else if (cmp > 0) {
                rhs.next();
            } else if (!cmp) {
                Range range = Range {position(), get_equal_range_length(),
                                     rhs.position(), rhs.get_equal_range_length()};
                set_cur_position(left_prev_pos);
                rhs.set_cur_position(right_prev_pos);
                return range;
            }
        }

        set_cur_position(left_prev_pos);
        rhs.set_cur_position(right_prev_pos);
        // tail range
        return Range {end(),  0, rhs.end(), 0};
    }

    template <bool left_nullable, bool right_nullable>
    int ALWAYS_INLINE _compare_at_cursor(const MergeJoinCursor& rhs) const {
        for (size_t i = 0; i < sort_columns_size(); ++i) {
            const auto * left_column = _get_cur_sort_column(i);
            const auto * right_column = rhs._get_cur_sort_column(i);

            int res = nullable_compare_at<left_nullable, right_nullable>(
                    *left_column, *right_column, position(), rhs.position());

            if (res) return res;
        }
        return 0;
    }

    bool ALWAYS_INLINE _is_same_prev(size_t lhs_pos) const {
        DCHECK_GT(lhs_pos, 0);
        for (size_t i = 0; i < sort_columns_size(); ++i)
            if (_get_cur_sort_column(i)->compare_at(
                        lhs_pos - 1, lhs_pos, *_get_cur_sort_column(i), 1) != 0)
                return false;
        return true;
    }

    bool _has_nullable_column = false;
};

// Node for sort merge join.
class VSortMergeJoinNode final : public VJoinNodeBase {
public:
    VSortMergeJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    Status alloc_resource(doris::RuntimeState* state) override;

    void release_resource(doris::RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

    bool can_push_more_data() const;

    Status close(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    void debug_string(int indentation_level, std::stringstream* out) const override;

    const RowDescriptor& intermediate_row_desc() const override {
        return *_intermediate_row_desc;
    }

    const RowDescriptor& row_desc() const override {
        return *_output_row_desc;
    }

private:
    Status _materialize_build_side(RuntimeState* state) override;

    void _add_tuple_is_null_column(Block* block) override;

    void _release_mem();

    void _construct_mutable_join_columns() {
        DCHECK_GT(_join_block.columns(), 0);
        _left_join_columns.clear();
        _right_join_columns.clear();

        MutableColumns total_columns = _join_block.mutate_columns();
        DCHECK_EQ(total_columns.size(), _num_left_columns + _num_right_columns);

        for (size_t i = 0; i < _num_left_columns; ++i) {
            _left_join_columns.push_back(std::move(total_columns[i]));
        }

        for (size_t i = 0; i < _num_right_columns; ++i) {
            _right_join_columns.push_back(std::move(total_columns[i + _num_left_columns]));
        }
    }

    void _construct_result_join_block() {
        MutableColumns total_columns(_num_left_columns + _num_right_columns);
        for (size_t i = 0; i < _num_left_columns; ++i) {
            total_columns[i] = std::move(_left_join_columns[i]);
        }

        for (size_t i = 0; i < _num_right_columns; ++i) {
            total_columns[i + _num_left_columns] = std::move(_right_join_columns[i]);
        }

        _join_block.set_columns(std::move(total_columns));
    }

    Status _do_sort_merge_join() {
        switch (_join_op) {
        case TJoinOp::type::INNER_JOIN:
            _inner_join();
            break;
        case TJoinOp::type::LEFT_OUTER_JOIN:
            _outer_join<true, false>();
            break;
        case TJoinOp::type::RIGHT_OUTER_JOIN:
            _outer_join<false, true>();
            break;
        case TJoinOp::type::FULL_OUTER_JOIN:
            _outer_join<true, true>();
            break;
        default:
            auto it = _TJoinOp_VALUES_TO_NAMES.find(_join_op);
            std::stringstream error_msg;
            const char* str = "unknown join op type ";

            if (it != _TJoinOp_VALUES_TO_NAMES.end()) {
                str = it->second;
            }

            error_msg << str << " not implemented";
            return Status::InternalError(error_msg.str());
        }

        return Status::OK();
    }

    void _inner_join() {
        do {
            Range range = _left_cursor->get_next_equal_range(*_right_cursor);

            // move left cursor to the position of `left_start`
            // move right cursor to the position of `right_start`
            _left_cursor->set_cur_position(range._left_start);
            _right_cursor->set_cur_position(range._right_start);

            if (range.is_empty()) {
                break;
            }

            _append_equal_segment(range);
            DCHECK_EQ(_left_join_columns[0]->size(), _right_join_columns[0]->size());
        } while (can_push_more_data());
    }

    template <bool is_left_outer_join, bool is_right_outer_join>
    void _outer_join() {
        do {
            Range range = _left_cursor->get_next_equal_range(*_right_cursor);

            if constexpr (is_left_outer_join) {
                _append_unequal_segment<false, true>(range);
            }

            if constexpr (is_right_outer_join) {
                _append_unequal_segment<true, false>(range);
            }

            if (range.is_empty()) {
                break;
            }

            _append_equal_segment(range);
            DCHECK_EQ(_left_join_columns[0]->size(), _right_join_columns[0]->size());
        } while (can_push_more_data());

        if constexpr (is_left_outer_join) {
            if (!_left_cursor->at_end()) {
                size_t length = _left_cursor->end() - _left_cursor->position();
                _append_left_range(_left_cursor->position(), length);
                _append_null_counterpart<false>(length);
                _left_cursor->next_n(length);
            }
        }

        if constexpr (is_right_outer_join) {
            if (!_right_cursor->at_end()) {
                size_t length = _right_cursor->end() - _right_cursor->position();
                _append_right_range(_right_cursor->position(), length);
                _append_null_counterpart<true>(length);
                _right_cursor->next_n(length);
            }
        }
    }

    void _append_equal_segment(Range& range) {
        size_t left_rows_to_append = range._left_length;
        size_t right_position = range._right_start;

        for (size_t right_row = 0; right_row < range._right_length; ++right_row, ++right_position) {
            _append_left_range(range._left_start, left_rows_to_append);
            _append_right_counterpart(right_position, left_rows_to_append);
        }

        _left_cursor->next_n(range._left_length);
        _right_cursor->next_n(range._right_length);
    }

    template <bool left_nullable, bool right_nullable>
    void _append_unequal_segment(Range& range) {
        if constexpr (left_nullable) {
            size_t length = range._right_start - _right_cursor->position();
            _append_right_range(_right_cursor->position(), length);
            _append_null_counterpart<true>(length);
            _right_cursor->next_n(length);
        }

        if constexpr (right_nullable) {
            size_t length = range._left_start - _left_cursor->position();
            _append_left_range(_left_cursor->position(), length);
            _append_null_counterpart<false>(length);
            _left_cursor->next_n(length);
        }
    }

    void _append_left_range(size_t start, size_t length) {
        Block block = _left_cursor->get_block_from_range(start, length);
        DCHECK_EQ(block.columns(), _num_left_columns);
        DCHECK_EQ(_left_join_columns.size(), _num_left_columns);

        for (size_t i = 0; i < _num_left_columns; ++i) {
            const auto & src_column = block.get_by_position(i).column;
            _left_join_columns[i]->insert_range_from(*src_column, 0, src_column->size());
        }
    }

    void _append_right_range(size_t start, size_t length) {
        Block block = _right_cursor->get_block_from_range(start, length);
        DCHECK_EQ(block.columns(), _num_right_columns);
        DCHECK_EQ(_right_join_columns.size(), _num_right_columns);

        for (size_t i = 0; i < _num_right_columns; ++i) {
            const auto & src_column = block.get_by_position(i).column;
            _right_join_columns[i]->insert_range_from(*src_column, 0, src_column->size());
        }
    }

    void _append_right_counterpart(size_t start, size_t length) {
        const Block* block;
        size_t row_position;
        std::tie(block, row_position) = _right_cursor->get_cur_row(start);

        for (size_t i = 0; i < _num_right_columns; ++i) {
            const auto & src_column = block->get_by_position(i).column;
            auto & dst_column = _right_join_columns[i];
            auto * dst_nullable = typeid_cast<ColumnNullable *>(dst_column.get());

            if (dst_nullable && !is_column_nullable(*src_column))
                dst_nullable->insert_many_from_not_nullable(*src_column, row_position, length);
            else
                dst_column->insert_many_from(*src_column, row_position, length);
        }
    }

    template <bool is_left>
    void _append_null_counterpart(size_t length) {
        if constexpr (is_left) {
            for (size_t i = 0; i < _num_left_columns; ++i) {
                auto & dst_column = _left_join_columns[i];
                auto * dst_nullable = typeid_cast<ColumnNullable *>(dst_column.get());
                dst_nullable->insert_many_defaults(length);
            }
        } else {
            for (size_t i = 0; i < _num_right_columns; ++i) {
                auto & dst_column = _right_join_columns[i];
                auto * dst_nullable = typeid_cast<ColumnNullable *>(dst_column.get());
                dst_nullable->insert_many_defaults(length);
            }
        }
    }

    size_t _num_left_columns = 0;
    size_t _num_right_columns = 0;

    // uint64_t _total_mem_usage = 0;

    MutableColumns _left_join_columns;
    MutableColumns _right_join_columns;

    std::unique_ptr<MergeJoinCursor> _left_cursor;
    std::unique_ptr<MergeJoinCursor> _right_cursor;

    // left expr
    VExprContextSPtrs _left_expr_ctxs;
    // right expr
    VExprContextSPtrs _right_expr_ctxs;
    // other expr
    VExprContextSPtrs _other_join_conjuncts;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    VExprContextSPtrs _filter_src_expr_ctxs;
    // bool _is_output_left_side_only = false;
    // VExprContextSPtrs _join_conjuncts;

    static constexpr size_t _SORT_MERGE_JOIN_BLOCK_SIZE_THRESHOLD = 10000;
};

} // namespace doris::vectorized
