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

#include "util/python_env.h"

#include "util/string_util.h"

namespace doris {

namespace fs = std::filesystem;

// extract python version by executing `python --version` and extract "3.9.16" from "Python 3.9.16"
// @param python_path: path to python executable, e.g. "/opt/miniconda3/envs/myenv/bin/python"
// @param version: extracted python version, e.g. "3.9.16"
static Status extract_python_version(const std::string& python_path, std::string* version) {
    static std::regex python_version_re(R"(^Python (\d+\.\d+\.\d+))");

    if (!fs::exists(python_path)) {
        return Status::NotFound("Python executable not found: {}", python_path);
    }

    std::string cmd = fmt::format("\"{}\" --version", python_path);
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        return Status::InternalError("Failed to run: {}", cmd);
    }

    std::string result;
    char buf[128];
    while (fgets(buf, sizeof(buf), pipe)) {
        result += buf;
    }
    pclose(pipe);

    std::smatch match;
    if (std::regex_search(result, match, python_version_re)) {
        *version = match[1].str();
        return Status::OK();
    }

    return Status::InternalError("Failed to extract Python version from path: {}, result: {}",
                                 python_path, result);
}

CondaEnvironment::CondaEnvironment(std::string name, std::string path, std::string python_path,
                                   std::string full_version, std::string major_minor_version,
                                   std::string dependency_path)
        : env_name(std::move(name)),
          env_base_path(std::move(path)),
          python_base_path(std::move(python_path)),
          python_full_version(std::move(full_version)),
          python_major_minor_version(std::move(major_minor_version)),
          python_dependency_path(std::move(dependency_path)) {}

PythonVersion CondaEnvironment::to_python_version() const {
    return PythonVersion(python_major_minor_version, python_full_version, env_base_path,
                         python_base_path);
}

std::string CondaEnvironment::to_string() const {
    return fmt::format(
            "[env_name: {}, env_base_path: {}, python_base_path: {}, python_full_version: {}, "
            "python_major_minor_version: {}, python_dependency_path: {}]",
            env_name, env_base_path, python_base_path, python_full_version,
            python_major_minor_version, python_dependency_path);
}

bool CondaEnvironment::is_valid() const {
    if (!fs::exists(env_base_path) || !fs::is_directory(env_base_path) ||
        !fs::exists(python_base_path) || !fs::is_regular_file(python_base_path) ||
        !fs::exists(python_dependency_path)) {
        return false;
    }

    auto perms = fs::status(python_base_path).permissions();
    if ((perms & fs::perms::owner_exec) == fs::perms::none) {
        return false;
    }

    std::string version;
    if (!extract_python_version(python_base_path, &version).ok()) {
        return false;
    }

    return python_full_version == version &&
           python_major_minor_version == version.substr(0, version.find_last_of('.'));
}

// Scan for environments under the /{conda_root_path}/envs directory from the conda root.
Status CondaEnvironment::scan_from_conda_root_path(const fs::path& conda_root_path,
                                                   std::vector<CondaEnvironment>* environments) {
    DCHECK(!conda_root_path.empty() && environments != nullptr);

    fs::path envs_dir = conda_root_path / "envs";
    if (!fs::exists(envs_dir) || !fs::is_directory(envs_dir)) {
        return Status::NotFound("Conda envs directory not found: {}", envs_dir.string());
    }

    for (const auto& entry : fs::directory_iterator(envs_dir)) {
        if (!entry.is_directory()) continue;

        std::string env_name = entry.path().filename(); // e.g. "myenv"
        std::string env_base_path = entry.path();       // e.g. "/opt/miniconda3/envs/myenv"
        std::string python_path =
                env_base_path + "/bin/python"; // e.g. "/{env_base_path}/bin/python"
        std::string python_version;            // e.g. "3.9.16"
        RETURN_IF_ERROR(extract_python_version(python_path, &python_version));
        size_t pos = python_version.find_last_of('.');
        if (UNLIKELY(pos == std::string::npos)) {
            return Status::InvalidArgument("Invalid python version: {}", python_version);
        }
        std::string python_major_minor_version = python_version.substr(0, pos); // e.g. "3.9"
        std::string python_dependency_path = fmt::format(
                "{}/lib/python{}/site-packages", env_base_path,
                python_major_minor_version); // e.g. "/{env_base_path}/lib/python3.9/site-packages"

        CondaEnvironment conda_env(env_name, env_base_path, python_path, python_version,
                                   python_major_minor_version, python_dependency_path);

        if (UNLIKELY(!conda_env.is_valid())) {
            LOG(WARNING) << "Invalid conda environment: " << conda_env.to_string();
            continue;
        }

        environments->push_back(std::move(conda_env));
    }

    return Status::OK();
}

Status CondaEnvScanner::scan() {
    if (!fs::exists(_conda_root_path)) {
        return Status::NotFound("Conda root path not found: {}", _conda_root_path.string());
    }
    RETURN_IF_ERROR(CondaEnvironment::scan_from_conda_root_path(_conda_root_path, &_conda_envs));
    if (_conda_envs.empty()) {
        return Status::NotFound("No conda environments found");
    }
    return Status::OK();
}

Status CondaEnvScanner::get_versions(std::vector<PythonVersion>* versions) const {
    DCHECK(versions != nullptr);
    if (_conda_envs.empty()) {
        return Status::InternalError("not found available version");
    }
    for (const auto& conda_env : _conda_envs) {
        versions->push_back(conda_env.to_python_version());
    }
    return Status::OK();
}

Status CondaEnvScanner::get_version(const std::string& runtime_version,
                                    PythonVersion* version) const {
    if (_conda_envs.empty()) {
        return Status::InternalError("not found available version");
    }
    std::string_view runtime_version_view(runtime_version);
    runtime_version_view = trim(runtime_version_view);
    for (const auto& conda_env : _conda_envs) {
        if (conda_env.python_full_version.starts_with(runtime_version_view)) {
            *version = conda_env.to_python_version();
            return Status::OK();
        }
    }
    return Status::NotFound("not found runtime version: {}", runtime_version);
}

Status CondaEnvScanner::default_version(PythonVersion* version) const {
    if (_conda_envs.empty()) {
        return Status::InternalError("not found available version");
    }
    *version = _conda_envs.begin()->to_python_version();
    return Status::OK();
}

std::string CondaEnvScanner::to_string() const {
    std::stringstream ss;
    ss << "Conda environments: ";
    for (const auto& conda_env : _conda_envs) {
        ss << conda_env.to_string() << ", ";
    }
    return ss.str();
}

Status PythonVersionManager::init(PythonEnvType env_type, const fs::path& root_path) {
    if (env_type == PythonEnvType::CONDA) {
        if (!fs::exists(root_path) || !fs::is_directory(root_path)) {
            return Status::InvalidArgument("Invalid conda root path: {}", root_path.string());
        }
        _env_scanner = std::make_shared<CondaEnvScanner>(root_path);
        RETURN_IF_ERROR(_env_scanner->scan());
    } else {
        return Status::NotSupported("Unsupported python runtime type: {}",
                                    static_cast<int>(env_type));
    }
    return Status::OK();
}

} // namespace doris
