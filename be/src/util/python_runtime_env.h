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

#include <arrow/type.h>
#include <sys/wait.h>
#include <memory>
#include "arrow/flight/client.h"
#include "common/status.h"
#include "udf/udf.h"
#include "util/arrow/row_batch.h"
#include "util/timezone_utils.h"
#include "vec/data_types/data_type.h"

namespace doris {

static std::string get_python_path(const std::string &path) {
    return fmt::format("{}/bin/python3", path);
}

struct PythonFunctionDescriptor {
    // used for reuse python env
    int32_t driver_id;
    std::string symbol;
    std::string location;
    std::string inline_code;
    std::string input_type;
    vectorized::DataTypePtr return_type;
    vectorized::DataTypes input_types;

    Status convert_type_to_schema(vectorized::DataTypePtr type, std::shared_ptr<arrow::Schema> *schema) {
        std::shared_ptr<arrow::DataType> arrow_type;
        arrow::SchemaBuilder builder;
        RETURN_IF_ERROR(convert_to_arrow_type(type, &arrow_type, TimezoneUtils::default_time_zone));
        std::shared_ptr<arrow::Field> result_field = std::make_shared<arrow::Field>("result", arrow_type, type->is_nullable());
        auto res = builder.AddField(result_field);
        auto build_res = builder.Finish();
        *schema = *build_res;
        return Status::OK();
    }

    Status serialize_to_json(std::string* json_str) const {
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();
        doc.AddMember("symbol", rapidjson::Value().SetString(symbol.c_str(), allocator), allocator);
        doc.AddMember("location", rapidjson::Value().SetString(location.c_str(), allocator),
                      allocator);
        doc.AddMember("input_type", rapidjson::Value().SetString(input_type.c_str(), allocator),
                      allocator);
        std::shared_ptr<arrow::DataType> arrow_return_type;
        RETURN_IF_ERROR(convert_to_arrow_type(return_type, &arrow_return_type, TimezoneUtils::default_time_zone));
        auto serialized_schema_result = arrow::ipc::SerializeSchema(*schema);
        std::shared_ptr<arrow::Buffer> serialized_schema;
        RETURN_IF_ARROW_ERROR(std::move(serialized_schema_result).Value(&serialized_schema));
        const uint8_t* data = serialized_schema->data();
        size_t serialized_size = serialized_schema->size();
        int base64_length = (size_t)(4.0 * ceil((double)serialized_size / 3.0)) + 1;
        char p[base64_length];
        int len = base64_encode2((unsigned char*)data, serialized_size, (unsigned char*)p);
        doc.AddMember("return_type", rapidjson::Value().SetString(p, len, allocator),
                        allocator);

        // Convert document to string
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);

        auto value = std::string(buffer.GetString(), buffer.GetSize());

        return value;
    }
};

class PythonProcessDescriptor {
public:
    PythonProcessDescriptor(pid_t worker_pid) : pid(worker_pid) {}

    ~PythonProcessDescriptor() { shutdown(); }

    void terminate() {
        if (pid == -1) return;
        kill(pid, SIGKILL);
    }

    void wait() {
        if (pid == -1) return;
        int status;
        waitpid(pid, &status, 0);
        pid = -1;
    }

    void shutdown() {
        std::call_once(once_flag, [this]() {
            terminate();
            wait();
            remove_unix_socket();
        });
    }

    void remove_unix_socket() {
        unlink(PyWorkerManager::unix_socket_path(_pid).c_str());
    }

    const std::string url() { return _url; }
    void set_url(std::string url) { _url = std::move(url); }

    void touch() { _last_touch_time = MonotonicSeconds(); }
    bool expired() { return MonotonicSeconds() - _last_touch_time > config::python_worker_expire_time_sec; }

    void mark_dead() { is_dead = true; }
    bool worker_is_dead() { return is_dead; }

private:
    std::once_flag once_flag;
    pid_t pid{-1};
    bool is_dead{false};
    std::string _url;
    int64_t _last_touch_time = 0;
};

class ArrowFlightClient {
public:
    using FlightStreamWriter = arrow::flight::FlightStreamWriter;
    using FlightStreamReader = arrow::flight::FlightStreamReader;
    using FlightClient = arrow::flight::FlightClient;
    using FlightLocation = arrow::flight::Location;
    using FlightDescriptor = arrow::flight::FlightDescriptor;

    Status init(const std::string& uri_string, const PythonFunctionDescriptor& func_desc,
                std::shared_ptr<PythonProcessDescriptor> process) {
        arrow::Result<FlightLocation> location_res = FlightLocation::Parse(uri_string);
        if (!location_res.ok()) {
            return Status::InvalidArgument("Failed to parse URI: {}", uri_string);
        }
        arrow::Result<std::unique_ptr<FlightClient>> client_res = FlightClient::Connect(*location_res);
        if (!client_res.ok()) {
            return Status::InvalidArgument("Failed to connect to URI: {}", uri_string);
        } 
        flight_client = std::move(*client_res);
        auto command = func_desc.to_json_string();
        FlightDescriptor descriptor = FlightDescriptor::Command(command);
        auto result = flight_client->DoExchange(descriptor);
        if (!result.ok()) {
            return Status::Corruption("Failed to DoExchange: {}", result.status().ToString());
        }
        flight_reader = std::move(result.reader);
        flight_writer = std::move(result.writer);
        process_descriptor = std::move(process);
        return Status::OK();
    }

    Status execute(arrow::RecordBatch& batch, std::shared_ptr<arrow::RecordBatch>* record_batch);

    void close();

private:
    bool begin = false;
    std::unique_ptr<FlightClient> flight_client;
    std::unique_ptr<FlightStreamWriter> flight_writer;
    std::unique_ptr<FlightStreamReader> flight_reader;
    std::shared_ptr<PythonProcessDescriptor> process_descriptor;
};

class PythonProcessManager {
public:
    using WorkerClientPtr = std::shared_ptr<ArrowFlightWithRW>;

    static PythonProcessManager& instance() {
        static PythonProcessManager instance;
        return instance;
    }

    StatusOr<WorkerClientPtr> get_client(const PyFunctionDescriptor& func_desc) {
        {
    std::shared_ptr<PyWorker> handle;
    std::string url;
    ASSIGN_OR_RETURN(handle, _acquire_worker(func_desc.driver_id, config::python_worker_reuse, &url));
    auto arrow_client = std::make_unique<ArrowFlightWithRW>();
    RETURN_IF_ERROR(arrow_client->init(url, func_desc, std::move(handle)));
    return arrow_client;
}
    }

    static std::string unix_socket(pid_t pid) {
        std::string unix_socket = fmt::format("grpc+unix://{}/pyworker_{}", config::local_library_dir, pid);
        return unix_socket;
    }

    static std::string unix_socket_path(pid_t pid) {
        std::string unix_socket_path = fmt::format("{}/pyworker_{}", config::local_library_dir, pid);
        return unix_socket_path;
    }

    static std::string server_path() {
        const char* server_filename = "flight_server.py";
        return fmt::format("{}/lib/py-packages/{}", std::getenv("DORIS_HOME"), server_filename);
    }

    void cleanup_expired_worker();

private:
    Status _fork_py_worker(ProcessPtr* child_process);
    Status _acquire_worker(int32_t driver_id, size_t reusable, std::string* url, std::shared_ptr<PythonProcessDescriptor>* descriptor);

    std::mutex mutex;
    std::unordered_map<int32_t, std::vector<std::shared_ptr<PythonProcessDescriptor>>> processes;
};

} // namespace doris
