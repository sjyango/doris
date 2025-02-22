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

#include "vec/runtime/vorc_transformer.h"

#include <glog/logging.h>
#include <stdlib.h>
#include <string.h>

#include <exception>
#include <ostream>

#include "io/fs/file_writer.h"
#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Vector.hh"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
VOrcOutputStream::VOrcOutputStream(doris::io::FileWriter* file_writer)
        : _file_writer(file_writer), _cur_pos(0), _written_len(0), _name("VOrcOutputStream") {}

VOrcOutputStream::~VOrcOutputStream() {
    if (!_is_closed) {
        close();
    }
}

void VOrcOutputStream::close() {
    if (!_is_closed) {
        Status st = _file_writer->close();
        _is_closed = true;
        if (!st.ok()) {
            LOG(WARNING) << "close orc output stream failed: " << st;
            throw std::runtime_error(st.to_string());
        }
    }
}

void VOrcOutputStream::write(const void* data, size_t length) {
    if (!_is_closed) {
        Status st = _file_writer->append({static_cast<const uint8_t*>(data), length});
        if (!st.ok()) {
            LOG(WARNING) << "Write to ORC file failed: " << st;
            return;
        }
        _cur_pos += length;
        _written_len += length;
    }
}

void VOrcOutputStream::set_written_len(int64_t written_len) {
    _written_len = written_len;
}

VOrcTransformer::VOrcTransformer(doris::io::FileWriter* file_writer,
                                 const VExprContextSPtrs& output_vexpr_ctxs,
                                 const std::string& schema, bool output_object_data)
        : VFileFormatTransformer(output_vexpr_ctxs, output_object_data),
          _file_writer(file_writer),
          _write_options(new orc::WriterOptions()),
          _schema_str(schema) {}

Status VOrcTransformer::open() {
    try {
        _schema = orc::Type::buildTypeFromString(_schema_str);
    } catch (const std::exception& e) {
        return Status::InternalError("Orc build schema from \"{}\" failed: {}", _schema_str,
                                     e.what());
    }
    _output_stream = std::unique_ptr<VOrcOutputStream>(new VOrcOutputStream(_file_writer));
    _writer = orc::createWriter(*_schema, _output_stream.get(), *_write_options);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

std::unique_ptr<orc::ColumnVectorBatch> VOrcTransformer::_create_row_batch(size_t sz) {
    return _writer->createRowBatch(sz);
}

int64_t VOrcTransformer::written_len() {
    return _output_stream->getLength();
}

Status VOrcTransformer::close() {
    if (_writer != nullptr) {
        try {
            _writer->close();
        } catch (const std::exception& e) {
            return Status::IOError(e.what());
        }
    }
    return Status::OK();
}

#define RETURN_WRONG_TYPE \
    return Status::InvalidArgument("Invalid column type: {}", raw_column->get_name());

#define WRITE_SINGLE_ELEMENTS_INTO_BATCH(VECTOR_BATCH, COLUMN)                                 \
    VECTOR_BATCH* cur_batch = dynamic_cast<VECTOR_BATCH*>(orc_col_batch);                      \
    if (null_map != nullptr) {                                                                 \
        cur_batch->hasNulls = true;                                                            \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();               \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                    \
            if (null_data[row_id] != 0) {                                                      \
                cur_batch->notNull[row_id] = 0;                                                \
            } else {                                                                           \
                cur_batch->notNull[row_id] = 1;                                                \
                cur_batch->data[row_id] = assert_cast<const COLUMN&>(*col).get_data()[row_id]; \
            }                                                                                  \
        }                                                                                      \
    } else if (const auto& not_null_column = check_and_get_column<const COLUMN>(col)) {        \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                    \
            cur_batch->data[row_id] = not_null_column->get_data()[row_id];                     \
        }                                                                                      \
    } else {                                                                                   \
        RETURN_WRONG_TYPE                                                                      \
    }

#define WRITE_CONTINUOUS_ELEMENTS_INTO_BATCH(VECTOR_BATCH, COLUMN, NATIVE_TYPE)                \
    VECTOR_BATCH* cur_batch = dynamic_cast<VECTOR_BATCH*>(orc_col_batch);                      \
    if (null_map != nullptr) {                                                                 \
        cur_batch->hasNulls = true;                                                            \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();               \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                    \
            if (null_data[row_id] != 0) {                                                      \
                cur_batch->notNull[row_id] = 0;                                                \
            } else {                                                                           \
                cur_batch->notNull[row_id] = 1;                                                \
                cur_batch->data[row_id] = assert_cast<const COLUMN&>(*col).get_data()[row_id]; \
            }                                                                                  \
        }                                                                                      \
    } else if (const auto& not_null_column = check_and_get_column<const COLUMN>(col)) {        \
        memcpy(cur_batch->data.data(), not_null_column->get_data().data(),                     \
               (end_row_id - start_row_id) * sizeof(NATIVE_TYPE));                             \
    } else {                                                                                   \
        RETURN_WRONG_TYPE                                                                      \
    }

#define WRITE_LARGEINT_STRING_INTO_BATCH(VECTOR_BATCH, COLUMN, BUFFER)                           \
    VECTOR_BATCH* cur_batch = dynamic_cast<VECTOR_BATCH*>(orc_col_batch);                        \
    const size_t begin_off = offset;                                                             \
    if (null_map != nullptr) {                                                                   \
        cur_batch->hasNulls = true;                                                              \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();                 \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                      \
            if (null_data[row_id] != 0) {                                                        \
                cur_batch->notNull[row_id] = 0;                                                  \
            } else {                                                                             \
                cur_batch->notNull[row_id] = 1;                                                  \
                auto value = assert_cast<const COLUMN&>(*col).get_data()[row_id];                \
                std::string value_str = fmt::format("{}", value);                                \
                size_t len = value_str.size();                                                   \
                while (BUFFER.size - BUFFER_RESERVED_SIZE < offset + len) {                      \
                    char* new_ptr = (char*)malloc(BUFFER.size + BUFFER_UNIT_SIZE);               \
                    memcpy(new_ptr, BUFFER.data, BUFFER.size);                                   \
                    free(const_cast<char*>(BUFFER.data));                                        \
                    BUFFER.data = new_ptr;                                                       \
                    BUFFER.size = BUFFER.size + BUFFER_UNIT_SIZE;                                \
                }                                                                                \
                strcpy(const_cast<char*>(BUFFER.data) + offset, value_str.c_str());              \
                offset += len;                                                                   \
                cur_batch->length[row_id] = len;                                                 \
            }                                                                                    \
        }                                                                                        \
        size_t data_off = 0;                                                                     \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                      \
            if (null_data[row_id] != 0) {                                                        \
                cur_batch->notNull[row_id] = 0;                                                  \
            } else {                                                                             \
                cur_batch->data[row_id] = const_cast<char*>(BUFFER.data) + begin_off + data_off; \
                data_off += cur_batch->length[row_id];                                           \
            }                                                                                    \
        }                                                                                        \
    } else if (const auto& not_null_column = check_and_get_column<const COLUMN>(col)) {          \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                      \
            auto value = not_null_column->get_data()[row_id];                                    \
            std::string value_str = fmt::format("{}", value);                                    \
            size_t len = value_str.size();                                                       \
            while (BUFFER.size - BUFFER_RESERVED_SIZE < offset + len) {                          \
                char* new_ptr = (char*)malloc(BUFFER.size + BUFFER_UNIT_SIZE);                   \
                memcpy(new_ptr, BUFFER.data, BUFFER.size);                                       \
                free(const_cast<char*>(BUFFER.data));                                            \
                BUFFER.data = new_ptr;                                                           \
                BUFFER.size = BUFFER.size + BUFFER_UNIT_SIZE;                                    \
            }                                                                                    \
            strcpy(const_cast<char*>(BUFFER.data) + offset, value_str.c_str());                  \
            offset += len;                                                                       \
            cur_batch->length[row_id] = len;                                                     \
        }                                                                                        \
        size_t data_off = 0;                                                                     \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                      \
            cur_batch->data[row_id] = const_cast<char*>(BUFFER.data) + begin_off + data_off;     \
            data_off += cur_batch->length[row_id];                                               \
        }                                                                                        \
    } else {                                                                                     \
        RETURN_WRONG_TYPE                                                                        \
    }

#define WRITE_DATE_STRING_INTO_BATCH(FROM, TO, BUFFER)                                             \
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);      \
    const size_t begin_off = offset;                                                               \
    if (null_map != nullptr) {                                                                     \
        cur_batch->hasNulls = true;                                                                \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();                   \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            if (null_data[row_id] != 0) {                                                          \
                cur_batch->notNull[row_id] = 0;                                                    \
            } else {                                                                               \
                cur_batch->notNull[row_id] = 1;                                                    \
                int len = binary_cast<FROM, TO>(                                                   \
                                  assert_cast<const ColumnVector<FROM>&>(*col).get_data()[row_id]) \
                                  .to_buffer(const_cast<char*>(BUFFER.data) + offset);             \
                while (BUFFER.size - BUFFER_RESERVED_SIZE < offset + len) {                        \
                    char* new_ptr = (char*)malloc(BUFFER.size + BUFFER_UNIT_SIZE);                 \
                    memcpy(new_ptr, BUFFER.data, BUFFER.size);                                     \
                    free(const_cast<char*>(BUFFER.data));                                          \
                    BUFFER.data = new_ptr;                                                         \
                    BUFFER.size = BUFFER.size + BUFFER_UNIT_SIZE;                                  \
                }                                                                                  \
                cur_batch->length[row_id] = len;                                                   \
                offset += len;                                                                     \
            }                                                                                      \
        }                                                                                          \
        size_t data_off = 0;                                                                       \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            if (null_data[row_id] != 0) {                                                          \
                cur_batch->notNull[row_id] = 0;                                                    \
            } else {                                                                               \
                cur_batch->data[row_id] = const_cast<char*>(BUFFER.data) + begin_off + data_off;   \
                data_off += cur_batch->length[row_id];                                             \
            }                                                                                      \
        }                                                                                          \
    } else if (const auto& not_null_column =                                                       \
                       check_and_get_column<const ColumnVector<FROM>>(col)) {                      \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            int len = binary_cast<FROM, TO>(not_null_column->get_data()[row_id])                   \
                              .to_buffer(const_cast<char*>(BUFFER.data) + offset);                 \
            while (BUFFER.size - BUFFER_RESERVED_SIZE < offset + len) {                            \
                char* new_ptr = (char*)malloc(BUFFER.size + BUFFER_UNIT_SIZE);                     \
                memcpy(new_ptr, BUFFER.data, BUFFER.size);                                         \
                free(const_cast<char*>(BUFFER.data));                                              \
                BUFFER.data = new_ptr;                                                             \
                BUFFER.size = BUFFER.size + BUFFER_UNIT_SIZE;                                      \
            }                                                                                      \
            cur_batch->length[row_id] = len;                                                       \
            offset += len;                                                                         \
        }                                                                                          \
        size_t data_off = 0;                                                                       \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            cur_batch->data[row_id] = const_cast<char*>(BUFFER.data) + begin_off + data_off;       \
            data_off += cur_batch->length[row_id];                                                 \
        }                                                                                          \
    } else {                                                                                       \
        RETURN_WRONG_TYPE                                                                          \
    }

#define WRITE_DATETIMEV2_STRING_INTO_BATCH(FROM, TO, BUFFER)                                       \
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);      \
    const size_t begin_off = offset;                                                               \
    if (null_map != nullptr) {                                                                     \
        cur_batch->hasNulls = true;                                                                \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();                   \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            if (null_data[row_id] != 0) {                                                          \
                cur_batch->notNull[row_id] = 0;                                                    \
            } else {                                                                               \
                cur_batch->notNull[row_id] = 1;                                                    \
                int output_scale = type_descriptor.scale;                                          \
                int len =                                                                          \
                        binary_cast<FROM, TO>(                                                     \
                                assert_cast<const ColumnVector<FROM>&>(*col).get_data()[row_id])   \
                                .to_buffer(const_cast<char*>(BUFFER.data) + offset, output_scale); \
                while (BUFFER.size - BUFFER_RESERVED_SIZE < offset + len) {                        \
                    char* new_ptr = (char*)malloc(BUFFER.size + BUFFER_UNIT_SIZE);                 \
                    memcpy(new_ptr, BUFFER.data, BUFFER.size);                                     \
                    free(const_cast<char*>(BUFFER.data));                                          \
                    BUFFER.data = new_ptr;                                                         \
                    BUFFER.size = BUFFER.size + BUFFER_UNIT_SIZE;                                  \
                }                                                                                  \
                cur_batch->length[row_id] = len;                                                   \
                offset += len;                                                                     \
            }                                                                                      \
        }                                                                                          \
        size_t data_off = 0;                                                                       \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            if (null_data[row_id] != 0) {                                                          \
                cur_batch->notNull[row_id] = 0;                                                    \
            } else {                                                                               \
                cur_batch->data[row_id] = const_cast<char*>(BUFFER.data) + begin_off + data_off;   \
                data_off += cur_batch->length[row_id];                                             \
            }                                                                                      \
        }                                                                                          \
    } else if (const auto& not_null_column =                                                       \
                       check_and_get_column<const ColumnVector<FROM>>(col)) {                      \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            int output_scale = type_descriptor.scale;                                              \
            int len = binary_cast<FROM, TO>(not_null_column->get_data()[row_id])                   \
                              .to_buffer(const_cast<char*>(BUFFER.data) + offset, output_scale);   \
            while (BUFFER.size - BUFFER_RESERVED_SIZE < offset + len) {                            \
                char* new_ptr = (char*)malloc(BUFFER.size + BUFFER_UNIT_SIZE);                     \
                memcpy(new_ptr, BUFFER.data, BUFFER.size);                                         \
                free(const_cast<char*>(BUFFER.data));                                              \
                BUFFER.data = new_ptr;                                                             \
                BUFFER.size = BUFFER.size + BUFFER_UNIT_SIZE;                                      \
            }                                                                                      \
            cur_batch->length[row_id] = len;                                                       \
            offset += len;                                                                         \
        }                                                                                          \
        size_t data_off = 0;                                                                       \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                        \
            cur_batch->data[row_id] = const_cast<char*>(BUFFER.data) + begin_off + data_off;       \
            data_off += cur_batch->length[row_id];                                                 \
        }                                                                                          \
    } else {                                                                                       \
        RETURN_WRONG_TYPE                                                                          \
    }

#define WRITE_DECIMAL_INTO_BATCH(VECTOR_BATCH, COLUMN)                                           \
    VECTOR_BATCH* cur_batch = dynamic_cast<VECTOR_BATCH*>(orc_col_batch);                        \
    if (null_map != nullptr) {                                                                   \
        cur_batch->hasNulls = true;                                                              \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();                 \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                      \
            if (null_data[row_id] != 0) {                                                        \
                cur_batch->notNull[row_id] = 0;                                                  \
            } else {                                                                             \
                cur_batch->notNull[row_id] = 1;                                                  \
                cur_batch->values[row_id] = assert_cast<const COLUMN&>(*col).get_data()[row_id]; \
            }                                                                                    \
        }                                                                                        \
    } else if (const auto& not_null_column = check_and_get_column<const COLUMN>(col)) {          \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {                      \
            cur_batch->values[row_id] = not_null_column->get_data()[row_id];                     \
        }                                                                                        \
    } else {                                                                                     \
        RETURN_WRONG_TYPE                                                                        \
    }

#define WRITE_COMPLEX_TYPE_INTO_BATCH(VECTOR_BATCH, COLUMN)                             \
    VECTOR_BATCH* cur_batch = dynamic_cast<VECTOR_BATCH*>(orc_col_batch);               \
    if (null_map != nullptr) {                                                          \
        cur_batch->hasNulls = true;                                                     \
        auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();        \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {             \
            if (null_data[row_id] != 0) {                                               \
                cur_batch->notNull[row_id] = 0;                                         \
            } else {                                                                    \
                cur_batch->notNull[row_id] = 1;                                         \
                const auto& ele = col->get_data_at(row_id);                             \
                cur_batch->data[row_id] = const_cast<char*>(ele.data);                  \
                cur_batch->length[row_id] = ele.size;                                   \
            }                                                                           \
        }                                                                               \
    } else if (const auto& not_null_column = check_and_get_column<const COLUMN>(col)) { \
        for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {             \
            const auto& ele = not_null_column->get_data_at(row_id);                     \
            cur_batch->data[row_id] = const_cast<char*>(ele.data);                      \
            cur_batch->length[row_id] = ele.size;                                       \
        }                                                                               \
    } else {                                                                            \
        RETURN_WRONG_TYPE                                                               \
    }

#define SET_NUM_ELEMENTS cur_batch->numElements = end_row_id - start_row_id;

Status VOrcTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // Buffer used by date/datetime/datev2/datetimev2/largeint type
    std::vector<StringRef> buffer_list;
    Defer defer {[&]() {
        for (auto& bufferRef : buffer_list) {
            if (bufferRef.data) {
                free(const_cast<char*>(bufferRef.data));
            }
        }
    }};

    size_t sz = block.rows();
    auto row_batch = _create_row_batch(sz);
    orc::StructVectorBatch* root = dynamic_cast<orc::StructVectorBatch*>(row_batch.get());
    try {
        for (size_t i = 0; i < block.columns(); i++) {
            auto& raw_column = block.get_by_position(i).column;
            _write_one_col(_output_vexpr_ctxs[i]->root()->type(), root->fields[i], raw_column, 0,
                           sz, &buffer_list);
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Orc write error: " << e.what();
        return Status::InternalError(e.what());
    }
    root->numElements = sz;
    _writer->add(*row_batch);
    _cur_written_rows += sz;

    return Status::OK();
}

Status VOrcTransformer::_write_one_col(const TypeDescriptor& type_descriptor,
                                       orc::ColumnVectorBatch* orc_col_batch,
                                       const ColumnPtr& raw_column, size_t start_row_id,
                                       size_t end_row_id, std::vector<StringRef>* buffer_list) {
    auto nullable = raw_column->is_nullable();
    const auto col = nullable ? reinterpret_cast<const ColumnNullable*>(raw_column.get())
                                        ->get_nested_column_ptr()
                                        .get()
                              : raw_column.get();
    auto null_map = nullable ? reinterpret_cast<const ColumnNullable*>(raw_column.get())
                                       ->get_null_map_column_ptr()
                             : nullptr;
    switch (type_descriptor.type) {
    case TYPE_BOOLEAN: {
        WRITE_SINGLE_ELEMENTS_INTO_BATCH(orc::LongVectorBatch, ColumnVector<UInt8>)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_TINYINT: {
        WRITE_SINGLE_ELEMENTS_INTO_BATCH(orc::LongVectorBatch, ColumnVector<Int8>)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_SMALLINT: {
        WRITE_SINGLE_ELEMENTS_INTO_BATCH(orc::LongVectorBatch, ColumnVector<Int16>)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_INT: {
        WRITE_SINGLE_ELEMENTS_INTO_BATCH(orc::LongVectorBatch, ColumnVector<Int32>)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_BIGINT: {
        WRITE_CONTINUOUS_ELEMENTS_INTO_BATCH(orc::LongVectorBatch, ColumnVector<Int64>, Int64)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_LARGEINT: {
        char* ptr = (char*)malloc(BUFFER_UNIT_SIZE);
        StringRef bufferRef;
        bufferRef.data = ptr;
        bufferRef.size = BUFFER_UNIT_SIZE;
        size_t offset = 0;
        WRITE_LARGEINT_STRING_INTO_BATCH(orc::StringVectorBatch, ColumnVector<Int128>, bufferRef)
        buffer_list->emplace_back(bufferRef);
        SET_NUM_ELEMENTS;
        break;
    }
    case TYPE_FLOAT: {
        WRITE_SINGLE_ELEMENTS_INTO_BATCH(orc::DoubleVectorBatch, ColumnVector<Float32>)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DOUBLE: {
        WRITE_CONTINUOUS_ELEMENTS_INTO_BATCH(orc::DoubleVectorBatch, ColumnVector<Float64>, Float64)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DATETIME:
    case TYPE_DATE: {
        char* ptr = (char*)malloc(BUFFER_UNIT_SIZE);
        StringRef bufferRef;
        bufferRef.data = ptr;
        bufferRef.size = BUFFER_UNIT_SIZE;
        size_t offset = 0;
        WRITE_DATE_STRING_INTO_BATCH(Int64, VecDateTimeValue, bufferRef)
        buffer_list->emplace_back(bufferRef);
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DATEV2: {
        char* ptr = (char*)malloc(BUFFER_UNIT_SIZE);
        StringRef bufferRef;
        bufferRef.data = ptr;
        bufferRef.size = BUFFER_UNIT_SIZE;
        size_t offset = 0;
        WRITE_DATE_STRING_INTO_BATCH(UInt32, DateV2Value<DateV2ValueType>, bufferRef)
        buffer_list->emplace_back(bufferRef);
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DATETIMEV2: {
        char* ptr = (char*)malloc(BUFFER_UNIT_SIZE);
        StringRef bufferRef;
        bufferRef.data = ptr;
        bufferRef.size = BUFFER_UNIT_SIZE;
        size_t offset = 0;
        WRITE_DATETIMEV2_STRING_INTO_BATCH(UInt64, DateV2Value<DateTimeV2ValueType>, bufferRef)
        buffer_list->emplace_back(bufferRef);
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_OBJECT: {
        if (_output_object_data) {
            WRITE_COMPLEX_TYPE_INTO_BATCH(orc::StringVectorBatch, ColumnBitmap)
            SET_NUM_ELEMENTS
        } else {
            RETURN_WRONG_TYPE
        }
        break;
    }
    case TYPE_HLL: {
        if (_output_object_data) {
            WRITE_COMPLEX_TYPE_INTO_BATCH(orc::StringVectorBatch, ColumnHLL)
            SET_NUM_ELEMENTS
        } else {
            RETURN_WRONG_TYPE
        }
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        WRITE_COMPLEX_TYPE_INTO_BATCH(orc::StringVectorBatch, ColumnString)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DECIMAL32: {
        WRITE_DECIMAL_INTO_BATCH(orc::Decimal64VectorBatch, ColumnDecimal32)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DECIMAL64: {
        WRITE_DECIMAL_INTO_BATCH(orc::Decimal64VectorBatch, ColumnDecimal64)
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DECIMALV2: {
        orc::Decimal128VectorBatch* cur_batch =
                dynamic_cast<orc::Decimal128VectorBatch*>(orc_col_batch);
        if (null_map != nullptr) {
            cur_batch->hasNulls = true;
            auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                if (null_data[row_id] != 0) {
                    cur_batch->notNull[row_id] = 0;
                } else {
                    cur_batch->notNull[row_id] = 1;
                    auto& v = assert_cast<const ColumnDecimal128&>(*col).get_data()[row_id];
                    orc::Int128 value(v >> 64, (uint64_t)v);
                    cur_batch->values[row_id] = value;
                }
            }
        } else if (const auto& not_null_column =
                           check_and_get_column<const ColumnDecimal128>(col)) {
            auto col_ptr = not_null_column->get_data().data();
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                auto v = col_ptr[row_id];
                orc::Int128 value(v >> 64, (uint64_t)v);
                cur_batch->values[row_id] = value;
            }
        } else {
            RETURN_WRONG_TYPE
        }
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_DECIMAL128I: {
        orc::Decimal128VectorBatch* cur_batch =
                dynamic_cast<orc::Decimal128VectorBatch*>(orc_col_batch);
        if (null_map != nullptr) {
            cur_batch->hasNulls = true;
            auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                if (null_data[row_id] != 0) {
                    cur_batch->notNull[row_id] = 0;
                } else {
                    cur_batch->notNull[row_id] = 1;
                    auto& v = assert_cast<const ColumnDecimal128I&>(*col).get_data()[row_id];
                    orc::Int128 value(v.value >> 64, (uint64_t)v.value);
                    cur_batch->values[row_id] = value;
                }
            }
        } else if (const auto& not_null_column =
                           check_and_get_column<const ColumnDecimal128I>(col)) {
            auto col_ptr = not_null_column->get_data().data();
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                auto v = col_ptr[row_id].value;
                orc::Int128 value(v >> 64, (uint64_t)v);
                cur_batch->values[row_id] = value;
            }
        } else {
            RETURN_WRONG_TYPE
        }
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_STRUCT: {
        orc::StructVectorBatch* cur_batch = dynamic_cast<orc::StructVectorBatch*>(orc_col_batch);

        if (null_map != nullptr) {
            cur_batch->hasNulls = true;
            auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();
            const ColumnStruct& struct_col = assert_cast<const ColumnStruct&>(*col);

            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                if (null_data[row_id] != 0) {
                    cur_batch->notNull[row_id] = 0;
                    for (int j = 0; j < struct_col.tuple_size(); ++j) {
                        cur_batch->fields[j]->hasNulls = true;
                        cur_batch->fields[j]->notNull[row_id] = 0;
                    }
                } else {
                    cur_batch->notNull[row_id] = 1;

                    for (int j = 0; j < struct_col.tuple_size(); ++j) {
                        _write_one_col(type_descriptor.children[j], cur_batch->fields[j],
                                       struct_col.get_column_ptr(j), row_id, row_id + 1,
                                       buffer_list);
                    }
                }
            }

        } else if (const auto& not_null_column = check_and_get_column<const ColumnStruct>(col)) {
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                for (int j = 0; j < not_null_column->tuple_size(); ++j) {
                    _write_one_col(type_descriptor.children[j], cur_batch->fields[j],
                                   not_null_column->get_column_ptr(j), row_id, row_id + 1,
                                   buffer_list);
                }
            }
        } else {
            RETURN_WRONG_TYPE
        }
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_MAP: {
        orc::MapVectorBatch* cur_batch = dynamic_cast<orc::MapVectorBatch*>(orc_col_batch);
        cur_batch->offsets[0] = 0;

        if (null_map != nullptr) {
            cur_batch->hasNulls = true;
            auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();

            const ColumnMap& map_column = assert_cast<const ColumnMap&>(*col);
            const ColumnArray::Offsets64& offsets = map_column.get_offsets();
            const ColumnPtr& nested_keys_column = map_column.get_keys_ptr();
            const ColumnPtr& nested_values_column = map_column.get_values_ptr();

            cur_batch->keys->resize(nested_keys_column->size());
            cur_batch->elements->resize(nested_values_column->size());
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                size_t offset = offsets[row_id - 1];
                size_t next_offset = offsets[row_id];
                if (null_data[row_id] != 0) {
                    cur_batch->notNull[row_id] = 0;
                } else {
                    cur_batch->notNull[row_id] = 1;
                    _write_one_col(type_descriptor.children[0], cur_batch->keys.get(),
                                   nested_keys_column, offset, next_offset, buffer_list);
                    _write_one_col(type_descriptor.children[1], cur_batch->elements.get(),
                                   nested_values_column, offset, next_offset, buffer_list);
                }
                cur_batch->offsets[row_id + 1] = next_offset;
            }
            cur_batch->keys->numElements = nested_keys_column->size();
            cur_batch->elements->numElements = nested_values_column->size();

        } else if (const auto& not_null_column = check_and_get_column<const ColumnMap>(col)) {
            const ColumnArray::Offsets64& offsets = not_null_column->get_offsets();
            const ColumnPtr& nested_keys_column = not_null_column->get_keys_ptr();
            const ColumnPtr& nested_values_column = not_null_column->get_values_ptr();

            cur_batch->keys->resize(nested_keys_column->size());
            cur_batch->elements->resize(nested_values_column->size());
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                size_t offset = offsets[row_id - 1];
                size_t next_offset = offsets[row_id];

                _write_one_col(type_descriptor.children[0], cur_batch->keys.get(),
                               nested_keys_column, offset, next_offset, buffer_list);
                _write_one_col(type_descriptor.children[1], cur_batch->elements.get(),
                               nested_values_column, offset, next_offset, buffer_list);

                cur_batch->offsets[row_id + 1] = next_offset;
            }
            cur_batch->keys->numElements = nested_keys_column->size();
            cur_batch->elements->numElements = nested_values_column->size();

        } else {
            RETURN_WRONG_TYPE
        }
        SET_NUM_ELEMENTS
        break;
    }
    case TYPE_ARRAY: {
        orc::ListVectorBatch* cur_batch = dynamic_cast<orc::ListVectorBatch*>(orc_col_batch);
        cur_batch->offsets[0] = 0;
        if (null_map != nullptr) {
            cur_batch->hasNulls = true;
            auto& null_data = assert_cast<const ColumnUInt8&>(*null_map).get_data();

            const ColumnArray& array_col = assert_cast<const ColumnArray&>(*col);
            const ColumnPtr& nested_column = array_col.get_data_ptr();

            cur_batch->elements->resize(nested_column->size());
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                auto& offsets = array_col.get_offsets();
                size_t offset = offsets[row_id - 1];
                size_t next_offset = offsets[row_id];
                if (null_data[row_id] != 0) {
                    cur_batch->notNull[row_id] = 0;
                } else {
                    cur_batch->notNull[row_id] = 1;
                    _write_one_col(type_descriptor.children[0], cur_batch->elements.get(),
                                   nested_column, offset, next_offset, buffer_list);
                }

                cur_batch->offsets[row_id + 1] = next_offset;
            }
            cur_batch->elements->numElements = nested_column->size();

        } else if (const auto& not_null_column = check_and_get_column<const ColumnArray>(col)) {
            auto& offsets = not_null_column->get_offsets();
            const ColumnPtr& nested_column = not_null_column->get_data_ptr();
            cur_batch->elements->resize(nested_column->size());
            for (size_t row_id = start_row_id; row_id < end_row_id; row_id++) {
                size_t offset = offsets[row_id - 1];
                size_t next_offset = offsets[row_id];
                _write_one_col(type_descriptor.children[0], cur_batch->elements.get(),
                               nested_column, offset, next_offset, buffer_list);

                cur_batch->offsets[row_id + 1] = next_offset;
            }
            cur_batch->elements->numElements = nested_column->size();
        } else {
            RETURN_WRONG_TYPE
        }
        SET_NUM_ELEMENTS
        break;
    }
    default: {
        return Status::InvalidArgument("Invalid expression type: {}",
                                       type_descriptor.debug_string());
    }
    }
    return Status::OK();
}

} // namespace doris::vectorized
