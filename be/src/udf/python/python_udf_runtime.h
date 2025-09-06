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

#include <boost/process.hpp>
#include <queue>

#include "common/status.h"
#include "util/python_env.h"
#include "util/slice.h"

namespace doris {

static const char* UNIX_SOCKET_PREFIX = "grpc+unix://";
static const char* BASE_UNIX_SOCKET_PATH_TEMPLATE = "{}{}/lib/udf/python/python_udf";
static const char* UNIX_SOCKET_PATH_TEMPLATE = "{}_{}.sock";
static const char* FLIGHT_SERVER_PATH_TEMPLATE = "{}/plugins/python_udf/{}";
static const char* FLIGHT_SERVER_FILENAME = "python_udf_server.py";
static const char* EXECUTABLE_PYTHON_FILENAME = "python";

inline std::string get_base_unix_socket_path() {
    return fmt::format(BASE_UNIX_SOCKET_PATH_TEMPLATE, UNIX_SOCKET_PREFIX,
                       std::getenv("DORIS_HOME"));
}

inline std::string get_unix_socket_path(pid_t child_pid) {
    return fmt::format(UNIX_SOCKET_PATH_TEMPLATE, get_base_unix_socket_path(), child_pid);
}

inline std::string get_unix_socket_file_path(pid_t child_pid) {
    return fmt::format(UNIX_SOCKET_PATH_TEMPLATE,
                       fmt::format(BASE_UNIX_SOCKET_PATH_TEMPLATE, "", std::getenv("DORIS_HOME")),
                       child_pid);
}

inline std::string get_fight_server_path() {
    return fmt::format(FLIGHT_SERVER_PATH_TEMPLATE, std::getenv("DORIS_HOME"),
                       FLIGHT_SERVER_FILENAME);
}

class PythonUDFProcess;
class PythonUDFProcessPool;

using ProcessPtr = std::unique_ptr<PythonUDFProcess>;
using PythonUDFProcessPoolPtr = std::unique_ptr<PythonUDFProcessPool>;

class PythonUDFProcess {
public:
    PythonUDFProcess(boost::process::child child, boost::process::ipstream output_stream,
                     PythonUDFProcessPool* pool)
            : _is_shutdown(false),
              _uri(get_unix_socket_path(child.id())),
              _unix_socket_file_path(get_unix_socket_file_path(child.id())),
              _child(std::move(child)),
              _output_stream(std::move(output_stream)),
              _pool(pool),
              _last_used(std::chrono::steady_clock::now()) {}

    ~PythonUDFProcess() { shutdown(); }

    std::string get_uri() const { return _uri; }

    const std::string& get_socket_file_path() const { return _unix_socket_file_path; }

    bool is_shutdown() const { return _is_shutdown; }

    bool is_alive() const {
        if (_is_shutdown) return false;
        return _child.running();
    }

    void remove_unix_socket();

    void shutdown();

    Slice get_python_process_log(bool blocking = false);

    std::string to_string() const;

    void set_last_used(std::chrono::steady_clock::time_point time_point) {
        _last_used = time_point;
    }

    std::chrono::steady_clock::time_point last_used() const { return _last_used; }

    PythonUDFProcessPool* pool() const { return _pool; }

private:
    constexpr static int TERMINATE_RETRY_TIMES = 10;
    constexpr static size_t MAX_ACCUMULATED_LOG_SIZE = 65536;

    bool _is_shutdown {false};
    std::string _uri;
    std::string _unix_socket_file_path;
    mutable boost::process::child _child;
    boost::process::ipstream _output_stream;
    std::string _accumulated_log;
    PythonUDFProcessPool* _pool {nullptr};
    std::chrono::steady_clock::time_point _last_used;
};

class PythonUDFProcessPool {
public:
    explicit PythonUDFProcessPool(PythonVersion version, size_t max_pool_size, size_t min_idle,
                                  std::chrono::seconds idle_timeout)
            : _python_version(version),
              _max_pool_size(max_pool_size),
              _init_pool_size(min_idle),
              _current_size(0),
              _is_shutdown(false),
              _idle_timeout(idle_timeout) {}

    explicit PythonUDFProcessPool(PythonVersion version)
            : _python_version(version),
              _max_pool_size(16),
              _init_pool_size(4),
              _current_size(0),
              _is_shutdown(false),
              _idle_timeout(std::chrono::seconds(60)) {}

    Status init();

    Status borrow_process(ProcessPtr* process);

    void return_process(ProcessPtr process);

    void shutdown();

    const PythonVersion& get_python_version() const { return _python_version; }

private:
    PythonVersion _python_version;
    size_t _max_pool_size;
    size_t _init_pool_size;
    size_t _current_size;
    bool _is_shutdown;
    std::chrono::seconds _idle_timeout;
    std::queue<ProcessPtr> _idle_processes;
    mutable std::mutex _mtx;
};

} // namespace doris