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

#include <filesystem>

#include "common/status.h"

namespace doris {

namespace fs = std::filesystem;

enum class PythonEnvType { CONDA, VIRTUAL_ENV };

struct PythonVersion {
    std::string version;         // e.g. "3.9"
    std::string full_version;    // e.g. "3.9.16"
    std::string base_path;       // e.g. "/root/anaconda3/envs/python3.9"
    std::string executable_path; // e.g. "{base_path}/bin/python3"

    PythonVersion() = default;

    PythonVersion(std::string version, std::string base_path)
            : version(std::move(version)), base_path(std::move(base_path)) {}

    explicit PythonVersion(std::string version, std::string full_version, std::string base_path)
            : version(std::move(version)),
              full_version(std::move(full_version)),
              base_path(std::move(base_path)) {}

    explicit PythonVersion(std::string version, std::string full_version, std::string base_path,
                           std::string executable_path)
            : version(std::move(version)),
              full_version(std::move(full_version)),
              base_path(std::move(base_path)),
              executable_path(std::move(executable_path)) {}
    
    bool operator==(const PythonVersion& other) const {
        return version == other.version && full_version == other.full_version;
    }

    const std::string& get_version() const { return version; }
    const std::string& get_base_path() const { return base_path; }
    std::string get_executable_path() const {
        return executable_path.empty() ? fmt::format("{}/bin/python3", base_path) : executable_path;
    }
    bool is_valid() const {
        return !version.empty() && !base_path.empty() && fs::exists(base_path);
    }
    bool executable_exists() const { return fs::exists(get_executable_path()); }
    std::string to_string() const {
        return fmt::format("[version: {}, full_version: {}, base_path: {}, executable_path: {}]",
                           version, full_version, base_path, executable_path);
    }
};

struct CondaEnvironment {
    std::string env_name;                   // e.g. "base" or "myenv"
    std::string env_base_path;              // e.g. "/opt/miniconda3/envs/myenv"
    std::string python_base_path;           // e.g. "/{env_base_path}/bin/python"
    std::string python_full_version;        // e.g. "3.9.16"
    std::string python_major_minor_version; // e.g. "3.9"
    std::string python_dependency_path;     // e.g. "/{env_base_path}/lib/python3.9/site-packages"

    explicit CondaEnvironment(std::string name, std::string path, std::string python_path,
                              std::string full_version, std::string major_minor_version,
                              std::string dependency_path);

    PythonVersion to_python_version() const;

    std::string to_string() const;

    bool is_valid() const;

    static Status scan_from_conda_root_path(const fs::path& conda_root_path,
                                            std::vector<CondaEnvironment>* environments);
};

class PythonEnvScanner {
public:
    virtual ~PythonEnvScanner() = default;
    virtual Status scan() = 0;
    virtual Status get_versions(std::vector<PythonVersion>* versions) const = 0;
    virtual Status get_version(const std::string& runtime_version,
                               PythonVersion* version) const = 0;
    virtual Status default_version(PythonVersion* version) const = 0;
    virtual std::string name() const = 0;
    virtual PythonEnvType env_type() const = 0;
    virtual std::string root_path() const = 0;
    virtual std::string to_string() const = 0;
};

class CondaEnvScanner : public PythonEnvScanner {
public:
    CondaEnvScanner(const fs::path& root_path) : _conda_root_path(root_path) {}

    ~CondaEnvScanner() override = default;

    Status scan() override;

    Status get_versions(std::vector<PythonVersion>* versions) const override;

    Status get_version(const std::string& runtime_version, PythonVersion* version) const override;

    Status default_version(PythonVersion* version) const override;

    std::string to_string() const override;

    std::string name() const override { return "conda"; }

    PythonEnvType env_type() const override { return PythonEnvType::CONDA; }

    std::string root_path() const override { return _conda_root_path.string(); }

private:
    fs::path _conda_root_path; // e.g. "/opt/miniconda3"
    std::vector<CondaEnvironment> _conda_envs;
};

class PythonVersionManager {
public:
    static PythonVersionManager& instance() {
        static PythonVersionManager instance;
        return instance;
    }

    Status init(PythonEnvType env_type, const fs::path& root_path);

    Status get_version(const std::string& runtime_version, PythonVersion* version) const {
        return _env_scanner->get_version(runtime_version, version);
    }

    Status default_version(PythonVersion* version) const {
        return _env_scanner->default_version(version);
    }

    std::string to_string() const { return _env_scanner->to_string(); }

private:
    // Run "python3 --version" to get the version output
    // static Status _query_actual_python_version(const std::string& executable_path,
    //                                            std::string& output);

    // Extract "3.9.16" from "Python 3.9.16"
    // static std::optional<std::string> _parse_version_from_output(const std::string& output);

    std::shared_ptr<PythonEnvScanner> _env_scanner;
};

} // namespace doris

namespace std {
template <>
struct hash<doris::PythonVersion> {
    size_t operator()(const doris::PythonVersion& v) const noexcept {
        return hash<string> {}(v.full_version);
    }
};
} // namespace std
