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

#include "udf/python/python_udf_server.h"

#include <butil/fd_utility.h>
#include <dirent.h>
#include <sys/poll.h>

#include <boost/asio.hpp>
#include <boost/process.hpp>

#include "udf/python/python_udf_client.h"
#include "util/python_env.h"

namespace doris {

Status PythonUDFServerManager::get_client(const PythonUDFMeta& func_meta,
                                          const PythonVersion& version,
                                          PythonUDFClientPtr* client) {
    std::lock_guard<std::mutex> lock(_pools_mutex);
    if (_pools.find(version) == _pools.end()) {
        PythonUDFProcessPoolPtr new_pool = std::make_unique<PythonUDFProcessPool>(version);
        RETURN_IF_ERROR(new_pool->init());
        _pools[version] = std::move(new_pool);
    }
    PythonUDFProcessPoolPtr& pool = _pools[version];
    ProcessPtr process;
    RETURN_IF_ERROR(pool->borrow_process(&process));
    RETURN_IF_ERROR(PythonUDFClient::create(func_meta, std::move(process), client));
    return Status::OK();
}

Status PythonUDFServerManager::fork(PythonUDFProcessPool* pool, ProcessPtr* process) {
    DCHECK(pool != nullptr);
    const PythonVersion& version = pool->get_python_version();
    // e.g. /usr/local/python3.7/bin/python3
    std::string python_executable_path = version.get_executable_path();
    // e.g. /{DORIS_HOME}/plugins/python_udf/python_udf_server.py
    std::string fight_server_path = get_fight_server_path();
    // e.g. grpc+unix:///home/doris/output/be/lib/udf/python/python_udf
    std::string base_unix_socket_path = get_base_unix_socket_path();
    // e.g. /usr/local/python3.7
    std::string python_home = version.get_base_path();
    std::vector<std::string> args = {"-u", // unbuffered output
                                     fight_server_path, base_unix_socket_path};
    boost::process::environment env = boost::this_process::environment();
    env["PYTHONHOME"] = python_home;
    boost::process::ipstream child_output; // input stream from child

    try {
        boost::process::child c(
                python_executable_path, args, boost::process::std_out > child_output,
                boost::process::env = env,
                boost::process::on_exit([](int exit_code, const std::error_code& ec) {
                    if (ec) {
                        LOG(WARNING) << "Python UDF server exited with error: " << ec.message();
                    }
                }));

        std::string log_line;
        std::string full_log;
        bool started_successfully = false;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::milliseconds(5000);

        while (std::chrono::steady_clock::now() - start < timeout) {
            if (std::getline(child_output, log_line)) {
                full_log += log_line + "\n";
                if (log_line == "Start python server successfully") {
                    started_successfully = true;
                    break;
                }
            } else {
                if (!c.running()) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }

        if (!started_successfully) {
            if (c.running()) {
                c.terminate(); // terminate() sends SIGTERM on Unix
                c.wait();      // wait for exit to avoid zombie processes
            }

            std::string error_msg = full_log.empty() ? "No output from Python server" : full_log;
            LOG(ERROR) << "Python server start failed:\n" << error_msg;
            return Status::InternalError("python server start failed:\n{}", error_msg);
        }

        *process = std::make_unique<PythonUDFProcess>(std::move(c), std::move(child_output), pool);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to start Python UDF server: {}", e.what());
    }

    return Status::OK();
}

Status PythonUDFServerManager::_close_unused_fds(const std::unordered_set<int>& reserved_fds) {
    DIR* dir = opendir("/proc/self/fd");
    auto defer = Defer([&dir]() {
        if (UNLIKELY(dir != nullptr)) {
            closedir(dir);
        }
    });

    if (UNLIKELY(dir == nullptr)) {
        return Status::InternalError(
                fmt::format("open /proc/self/fd error: {}", std::strerror(errno)));
    }

    int dir_fd = dirfd(dir);
    if (UNLIKELY(dir_fd < 0)) {
        return Status::InternalError(fmt::format("syscall dirfd error: {}", std::strerror(errno)));
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_LNK) {
            int fd = atoi(entry->d_name);
            if (fd >= 0 && fd != dir_fd && reserved_fds.count(fd) == 0) {
                close(fd);
            }
        }
    }

    return Status::OK();
}

void PythonUDFServerManager::shutdown() {
    std::lock_guard lock(_pools_mutex);
    for (auto& pool : _pools) {
        pool.second->shutdown();
    }
    _pools.clear();
    LOG(INFO) << "Python UDF server manager shutdown successfully";
}

} // namespace doris