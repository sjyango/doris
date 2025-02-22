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

#include <fmt/format.h>
#include <stddef.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "exec/odbc_connector.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {
namespace vectorized {

class Block;

class VOdbcTableWriter final : public AsyncResultWriter, public ODBCConnector {
public:
    static ODBCConnectorParam create_connect_param(const TDataSink&);

    VOdbcTableWriter(const doris::TDataSink& t_sink, const VExprContextSPtrs& output_exprs);

    // connect to odbc server
    Status open(RuntimeState* state, RuntimeProfile* profile) override {
        RETURN_IF_ERROR(ODBCConnector::open(state, false));
        return init_to_write(profile);
    }

    Status append_block(vectorized::Block& block) override;

    Status close(Status s) override { return ODBCConnector::close(s); }

    bool in_transaction() override { return TableConnector::_is_in_transaction; }

    Status commit_trans() override { return ODBCConnector::finish_trans(); }
};
} // namespace vectorized
} // namespace doris