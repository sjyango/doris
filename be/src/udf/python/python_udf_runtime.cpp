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

#include "udf/python/python_udf_runtime.h"

#include <butil/fd_utility.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/process.hpp>

#include "common/logging.h"
#include "common/status.h"
#include "udf/python/python_udf_server.h"

namespace doris {

void PythonUDFProcess::remove_unix_socket() {
    if (_uri.empty() || _unix_socket_file_path.empty()) return;

    if (unlink(_unix_socket_file_path.c_str()) == 0) {
        LOG(INFO) << "Successfully removed unix socket: " << _unix_socket_file_path;
        return;
    }

    if (errno == ENOENT) {
        // File does not exist, this is fine, no need to warn
        LOG(INFO) << "Unix socket not found (already removed): " << _uri;
    } else {
        LOG(WARNING) << "Failed to remove unix socket " << _uri << ": " << std::strerror(errno)
                     << " (errno=" << errno << ")";
    }
}

void PythonUDFProcess::shutdown() {
    if (!_child.valid() || _is_shutdown) return;

    _child.terminate();
    bool graceful = false;
    constexpr std::chrono::milliseconds retry_interval(100); // 100ms

    for (int i = 0; i < TERMINATE_RETRY_TIMES; ++i) {
        if (!_child.running()) {
            graceful = true;
            break;
        }
        std::this_thread::sleep_for(retry_interval);
    }

    if (!graceful) {
        LOG(WARNING) << "Python process did not terminate gracefully, sending SIGKILL";
        ::kill(_child.id(), SIGKILL);
        _child.wait();
    }

    if (int exit_code = _child.exit_code(); exit_code > 128 && exit_code <= 255) {
        int signal = exit_code - 128;
        LOG(INFO) << "Python process was killed by signal " << signal;
    } else {
        LOG(INFO) << "Python process exited normally with code: " << exit_code;
    }

    _output_stream.close();
    remove_unix_socket();
    _is_shutdown = true;
}

Slice PythonUDFProcess::get_python_process_log(bool blocking) {
    if (_is_shutdown) {
        return Slice(_accumulated_log);
    }

    if (blocking && _child.valid() && _child.running()) {
        _child.wait();
    }

    std::string line;
    while (_output_stream.good()) {
        // Check if there's data available: peek() returns EOF if no data
        if (_output_stream.peek() == std::char_traits<char>::eof()) {
            break;
        }

        if (std::getline(_output_stream, line)) {
            _accumulated_log += line + "\n";
        } else {
            break;
        }

        // Prevent log from growing too large
        if (_accumulated_log.size() >= MAX_ACCUMULATED_LOG_SIZE) {
            // Keep only the second half
            size_t half = MAX_ACCUMULATED_LOG_SIZE / 2;
            _accumulated_log = _accumulated_log.substr(_accumulated_log.size() - half);
        }
    }

    return Slice(_accumulated_log.data(), _accumulated_log.size());
}

std::string PythonUDFProcess::to_string() const {
    return fmt::format(
            "PythonUDFProcess(child_pid={}, uri={}, "
            "unix_socket_file_path={}, is_shutdown={})",
            _child.id(), _uri, _unix_socket_file_path, _is_shutdown);
}

Status PythonUDFProcessPool::init() {
    if (_init_pool_size > _max_pool_size) {
        return Status::InvalidArgument("min_idle cannot be greater than max_pool_size");
    }

    std::lock_guard<std::mutex> lock(_mtx);
    for (size_t i = 0; i < _init_pool_size; ++i) {
        ProcessPtr process;
        RETURN_IF_ERROR(PythonUDFServerManager::instance().fork(this, &process));
        process->set_last_used(std::chrono::steady_clock::now());
        _idle_processes.push(std::move(process));
        ++_current_size;
    }

    return Status::OK();
}

Status PythonUDFProcessPool::borrow_process(ProcessPtr* process) {
    if (_is_shutdown) {
        return Status::RuntimeError("UDF process pool is shutdown");
    }

    std::lock_guard<std::mutex> lock(_mtx);

    if (!_idle_processes.empty()) {
        *process = std::move(_idle_processes.front());
        _idle_processes.pop();
        return Status::OK();
    }

    if (_current_size >= _max_pool_size) {
        return Status::RuntimeError("UDF process pool exhausted (max size = {})", _max_pool_size);
    }

    RETURN_IF_ERROR(PythonUDFServerManager::instance().fork(this, process));
    (*process)->set_last_used(std::chrono::steady_clock::now());
    ++_current_size;
    return Status::OK();
}

void PythonUDFProcessPool::return_process(ProcessPtr process) {
    if (!process || _is_shutdown) return;

    if (!process->is_alive()) {
        --_current_size;
        LOG(WARNING) << "return dead process: " << process->to_string();
        return;
    }

    process->set_last_used(std::chrono::steady_clock::now());
    std::lock_guard<std::mutex> lock(_mtx);
    _idle_processes.push(std::move(process));
}

void PythonUDFProcessPool::shutdown() {
    if (_is_shutdown) return;
    std::lock_guard<std::mutex> lock(_mtx);
    while (!_idle_processes.empty()) {
        _idle_processes.front()->shutdown();
        _idle_processes.pop();
    }
    _is_shutdown = true;
}

} // namespace doris