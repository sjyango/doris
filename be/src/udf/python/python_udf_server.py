# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import base64
import datetime
import importlib.util
import inspect
import json
import sys
import os
import traceback
import logging
import typing
import time
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Tuple, get_origin
from types import ModuleType
from datetime import datetime
from enum import Enum

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s',
    handlers=[
        logging.FileHandler("/data/workspace/doris/output/be/lib/udf/python/debug_output.log", mode='a', encoding='utf-8'),
    ]
)


UNIX_SOCKET_PATH: str = None


def extract_base_unix_socket_path(unix_socket_uri: str) -> str:
    if unix_socket_uri.startswith("grpc+unix://"):
        unix_socket_uri = unix_socket_uri[len("grpc+unix://"):]
    return unix_socket_uri


def remove_unix_socket(unix_socket_uri: str) -> None:
    if unix_socket_uri is None:
        return
    base_unix_socket_path = extract_base_unix_socket_path(unix_socket_uri)
    if os.path.exists(base_unix_socket_path):
        os.unlink(base_unix_socket_path)
        logging.info(f"Removed UNIX socket {base_unix_socket_path} successfully")
    else:
        logging.warning(f"UNIX socket {base_unix_socket_path} does not exist")


def monitor_parent_exit():
    parent_pid = os.getppid()
    if parent_pid == 1:
        # Parent process is init, no need to monitor
        return

    while True:
        try:
            # os.kill(pid, 0) only checks whether the process exists without sending an actual signal
            os.kill(parent_pid, 0)
        except OSError:
            # Parent process died
            global UNIX_SOCKET_PATH
            remove_unix_socket(UNIX_SOCKET_PATH)
            logging.error(f"Parent process {parent_pid} died, exiting UDF server, unix socket path: {UNIX_SOCKET_PATH}")
            os._exit(0)
        # Check every 2 seconds
        time.sleep(2)

monitor_thread = threading.Thread(target=monitor_parent_exit, daemon=True)
monitor_thread.start()


class VectorType(Enum):
    LIST = "list"
    TUPLE = "tuple"
    PANDAS_SERIES = "pandas.Series"
    ARROW_ARRAY = "pyarrow.Array"
    ARROW_CHUNKED_ARRAY = "pyarrow.ChunkedArray"

    @property
    def python_type(self):
        mapping = {
            VectorType.LIST: list,
            VectorType.TUPLE: tuple,
            VectorType.PANDAS_SERIES: pd.Series,
            VectorType.ARROW_ARRAY: pa.Array,
            VectorType.ARROW_CHUNKED_ARRAY: pa.ChunkedArray,
        }
        return mapping[self]
    
    @staticmethod
    def resolve_vector_type(param: inspect.Parameter):
        """
        Resolves the param's type annotation to the corresponding VectorType enum.
        Returns None if the type is unsupported or not a vector type.
        """
        if param is None or param.annotation is None or param.annotation is inspect.Parameter.empty:
            return None

        name = param.name
        annotation = param.annotation
        logging.info(f"Resolve param type {name}: {annotation}, type: {type(annotation)}")

        origin = get_origin(annotation)
        raw_type = origin if origin is not None else annotation

        if raw_type is list:
            return VectorType.LIST
        if raw_type is tuple:
            return VectorType.TUPLE
        if raw_type is pd.Series:
            return VectorType.PANDAS_SERIES
        if raw_type is pa.Array:
            return VectorType.ARROW_ARRAY
        if raw_type is pa.ChunkedArray:
            return VectorType.ARROW_CHUNKED_ARRAY
        if raw_type is typing.List:
            return VectorType.LIST
        if raw_type is typing.Tuple:
            return VectorType.TUPLE

        return None


class PythonUDFMeta:
    """Metadata container for a Python UDF."""
    
    def __init__(
        self,
        name: str,
        symbol: str,
        location: str,
        runtime_version: str,
        always_nullable: bool,
        inline_code: bytes,
        input_types: pa.Schema,
        output_type: pa.DataType,
    ) -> None:
        self.name = name
        self.symbol = symbol
        self.location = location
        self.runtime_version = runtime_version
        self.always_nullable = always_nullable
        self.inline_code = inline_code
        self.input_types = input_types
        self.output_type = output_type

    def __str__(self) -> str:
        return (
            f"PythonUDFMeta(name={self.name}, symbol={self.symbol}, "
            f"location={self.location}, runtime_version={self.runtime_version}, "
            f"always_nullable={self.always_nullable}, inline_code={self.inline_code}, "
            f"input_types={self.input_types}, output_type={self.output_type})"
        )


class AdaptivePythonUDF:
    """
    A wrapper around a UDF function that supports both scalar and vectorized execution modes.
    The mode is determined by the type hints of the function parameters.
    """

    def __init__(self, python_udf_meta: PythonUDFMeta, func: Callable) -> None:
        self.python_udf_meta = python_udf_meta
        self._eval_func = func

    def __str__(self) -> str:
        input_type_strs = [str(t) for t in self.python_udf_meta.input_types.types]
        output_type_str = str(self.python_udf_meta.output_type)
        eval_func_str = f"{self.python_udf_meta.name}({', '.join(input_type_strs)}) -> {output_type_str}"
        return f"AdaptivePythonUDF(python_udf_meta: {self.python_udf_meta}, eval_func: {eval_func_str})"

    def __call__(self, record_batch: pa.RecordBatch) -> pa.Array:
        """
        Executes the UDF on the given record batch. Supports both scalar and vectorized modes.

        :param record_batch: Input data with N columns, each of length num_rows
        :return: Output array of length num_rows
        """
        if record_batch.num_rows == 0:
            return pa.array([], type=self._get_output_type())

        if self._should_use_vectorized():
            logging.info(f"Using vectorized mode for UDF: {self.python_udf_meta.name}")
            return self._vectorized_call(record_batch)
        else:
            logging.info(f"Using scalar mode for UDF: {self.python_udf_meta.name}")
            return self._scalar_call(record_batch)
    
    @staticmethod
    def _cast_arrow_to_vector(arrow_array: pa.Array, vec_type: VectorType):
        """
        Convert a pa.Array to an instance of the specified VectorType.
        """
        if vec_type == VectorType.LIST:
            return arrow_array.to_pylist()
        elif vec_type == VectorType.TUPLE:
            return tuple(arrow_array.to_pylist())
        elif vec_type == VectorType.PANDAS_SERIES:
            return arrow_array.to_pandas()
        elif vec_type in (VectorType.ARROW_ARRAY, VectorType.ARROW_CHUNKED_ARRAY):
            return arrow_array
        else:
            raise ValueError(f"Unsupported vector type: {vec_type}")

    def _should_use_vectorized(self) -> bool:
        """
        Determines whether to use vectorized mode based on parameter type annotations.
        Returns True if any parameter is annotated as:
            - List[T], list
            - pa.Array, pa.ChunkedArray
            - pd.Series
            - np.ndarray (optional)
        """
        try:
            signature = inspect.signature(self._eval_func)
        except ValueError:
            # Cannot inspect built-in or C functions; default to scalar
            return False

        for param in signature.parameters.values():
            if VectorType.resolve_vector_type(param):
                return True

        return False

    def _convert_from_arrow_to_py(self, field):
        if field is None:
            return None
        logging.info(f"Converting arrow to py: {field}, type: {type(field)}, arrow_version: {pa.__version__}")
        if pa.types.is_map(field.type):
            # pyarrow.lib.MapScalar's as_py() returns a list of tuples, convert to dict
            list_of_tuples = field.as_py()
            return dict(list_of_tuples) if list_of_tuples is not None else None
        return field.as_py()

    def _scalar_call(self, record_batch: pa.RecordBatch) -> pa.Array:
        """
        Applies the UDF in scalar mode: one row at a time.
        """
        columns = record_batch.columns
        num_rows = record_batch.num_rows
        result = []

        for i in range(num_rows):
            args = [self._convert_from_arrow_to_py(col[i]) for col in columns]

            for j, arg in enumerate(args):
                logging.info(f"Scalar call with arg {j}: {arg}, type: {type(arg)}, value: {arg}")

            try:
                res = self._eval_func(*args)
                result.append(res)
            except Exception as e:
                logging.error(f"Error in scalar UDF execution at row {i}: {e}")
                result.append(None)

        return pa.array(result, type=self._get_output_type(), from_pandas=True)

    def _vectorized_call(self, record_batch: pa.RecordBatch) -> pa.Array:
        args = record_batch.columns
        logging.info(f"Vectorized call with {len(args)} columns")

        sig = inspect.signature(self._eval_func)
        params = list(sig.parameters.values())

        if len(args) != len(params):
            raise ValueError(f"UDF expects {len(params)} args, got {len(args)}")

        converted_args = []
        for param, arrow_col in zip(params, args):
            vec_type = VectorType.resolve_vector_type(param)

            if vec_type is None:
                converted = arrow_col.to_pylist()
                logging.info(f"Fallback to list for {param.name}")
            else:
                converted = self._cast_arrow_to_vector(arrow_col, vec_type)
                logging.info(f"Converted {param.name}: {vec_type}")

            converted_args.append(converted)

        try:
            result = self._eval_func(*converted_args)
        except Exception as e:
            raise RuntimeError(f"Error in vectorized UDF: {e}") from e

        if isinstance(result, pa.Array):
            return result
        elif isinstance(result, list):
            return pa.array(result, type=self._get_output_type(), from_pandas=True)
        elif hasattr(result, "__array__"):
            return pa.array(result, type=self._get_output_type())
        else:
            out_type = self._get_output_type()
            return pa.array([result] * record_batch.num_rows, type=out_type)

    def _get_output_type(self) -> pa.DataType:
        return self.python_udf_meta.output_type or pa.null()


class UDFLoader(ABC):
    """Abstract base class for loading UDFs from different sources."""
    
    def __init__(self, python_udf_meta: PythonUDFMeta) -> None:
        self.python_udf_meta = python_udf_meta

    @abstractmethod
    def load(self) -> AdaptivePythonUDF:
        raise NotImplementedError("Subclasses must implement load().")


class InlineUDFLoader(UDFLoader):
    """Loads a UDF defined directly in inline code."""
    
    def load(self) -> AdaptivePythonUDF:
        symbol = self.python_udf_meta.symbol
        inline_code = self.python_udf_meta.inline_code.decode('utf-8')
        env: dict[str, Any] = {}
        logging.info(f"Loading inline code: {inline_code}")
        try:
            exec(inline_code, env)
        except Exception as e:
            logging.error(f"Failed to exec inline code: {e}")
            raise RuntimeError(f"Failed to exec inline code: {e}") from e
        logging.info(f"Executing inline code: {inline_code}")
        func = env.get(symbol)
        if not callable(func):
            logging.error(f"Function '{symbol}' not found in inline code.")
            raise ValueError(f"Function '{symbol}' not found in inline code.")

        logging.info(f"Loaded function successfully '{symbol}' from inline code.")
        return AdaptivePythonUDF(self.python_udf_meta, func)


class ModuleUDFLoader(UDFLoader):
    """Loads a UDF from a Python module file (.py)."""
    
    def load(self) -> AdaptivePythonUDF:
        symbol = self.python_udf_meta.symbol
        location = self.python_udf_meta.location

        if not symbol or '.' not in symbol:
            raise ValueError(
                f"Invalid symbol: '{symbol}'. Must be in 'module.function' format"
            )

        try:
            module_name, func_name = symbol.rsplit(".", 1)
        except ValueError:
            raise ValueError(
                f"Invalid symbol format: '{symbol}'. "
                "Expected 'module.function' (e.g., 'main.my_udf')"
            ) from None

        location = os.path.join(os.path.normpath(location), module_name + ".py")
        if not os.path.exists(location):
            raise ValueError(f"Module file not found: {location}")

        logging.info(f"Loading module: {location}")

        spec = importlib.util.spec_from_file_location(module_name, location)
        if not spec or not spec.loader:
            raise ImportError(f"Could not load module spec from {location}")

        module: ModuleType = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        func = getattr(module, func_name, None)
        if not callable(func):
            raise ValueError(f"Function '{symbol}' not found in module: {location}")

        logging.info(f"Loaded function '{symbol}' from module: {location}")
        return AdaptivePythonUDF(self.python_udf_meta, func)


class UDFLoaderFactory:
    """Factory to select the appropriate loader based on UDF location."""
    
    @staticmethod
    def get_loader(python_udf_meta: PythonUDFMeta) -> UDFLoader:
        location = python_udf_meta.location
        if location.lower().strip() == "inline":
            return InlineUDFLoader(python_udf_meta)
        
        if UDFLoaderFactory.check_module(location):
            return ModuleUDFLoader(python_udf_meta)
        else:
            raise ValueError(f"Unsupported UDF location: {location}")

    def check_module(location: str) -> bool:
        """
        Checks if a location is a valid Python module or package.
        
        A valid module is either:
        - A .py file, or
        - A directory containing __init__.py (i.e., a package).
        
        Raises:
            ValueError: If the location does not exist or contains no Python module.
        
        Returns:
            True if valid.
        """
        if not os.path.exists(location):
            raise ValueError(f"Module not found: {location}")

        if os.path.isfile(location):
            if location.endswith('.py'):
                return True
            else:
                raise ValueError(f"File is not a Python module (.py): {location}")

        if os.path.isdir(location):
            # Check if any .py file exists (including __init__.py)
            has_py_file = any(
                f.endswith('.py') for f in os.listdir(location)
                if os.path.isfile(os.path.join(location, f))
            )
            if has_py_file:
                return True
            else:
                raise ValueError(f"Directory contains no Python (.py) files: {location}")

        raise ValueError(f"Invalid module location (not file or directory): {location}")


class UDFFlightServer(flight.FlightServerBase):
    """Arrow Flight server for executing Python UDFs."""

    @staticmethod
    def parse_python_udf_meta(descriptor: flight.FlightDescriptor) -> Optional[PythonUDFMeta]:
        """Parses UDF metadata from a command descriptor."""
        if descriptor.descriptor_type != flight.DescriptorType.CMD:
            logging.error(f"Invalid descriptor type: {descriptor.descriptor_type}")
            return None

        cmd_json = json.loads(descriptor.command)
        name = cmd_json["name"]
        symbol = cmd_json["symbol"]
        location = cmd_json["location"]
        runtime_version = cmd_json["runtime_version"]
        always_nullable = cmd_json["always_nullable"]

        inline_code = base64.b64decode(cmd_json["inline_code"])
        input_binary = base64.b64decode(cmd_json["input_types"])
        output_binary = base64.b64decode(cmd_json["return_type"])

        input_schema = pa.ipc.read_schema(pa.BufferReader(input_binary))
        output_schema = pa.ipc.read_schema(pa.BufferReader(output_binary))

        if len(output_schema) != 1:
            logging.error(f"Output schema must have exactly one field: {output_schema}")
            return None

        output_type = output_schema.field(0).type

        logging.info(
            f"Parsed UDF: name={name}, symbol={symbol}, location={location}, "
            f"input_types={input_schema}, output_type={output_type}"
        )

        return PythonUDFMeta(
            name=name,
            symbol=symbol,
            location=location,
            runtime_version=runtime_version,
            always_nullable=always_nullable,
            inline_code=inline_code,
            input_types=input_schema,
            output_type=output_type,
        )


    @staticmethod
    def check_schema_compatibility(
        record_batch: pa.RecordBatch,
        expected_schema: pa.Schema
    ) -> Tuple[bool, str]:
        """
        Validates that the input RecordBatch schema matches the expected schema with type compatibility.
        
        :return: (is_compatible, error_message)
        """
        actual = record_batch.schema
        expected = expected_schema

        if len(actual) != len(expected):
            return False, f"Schema length mismatch: got {actual}, expected {expected}"

        for i, (actual_field, expected_field) in enumerate(zip(actual, expected)):
            if not UDFFlightServer.is_type_compatible(actual_field.type, expected_field.type):
                return False, (
                    f"Type mismatch for field index {i}: "
                    f"got {actual_field.type}, expected {expected_field.type}"
                )

        return True, ""

    @staticmethod
    def is_type_compatible(actual: pa.DataType, expected: pa.DataType) -> bool:
        """
        Checks if the actual type can be safely promoted to the expected type.
        Supports common implicit conversions.
        """
        if actual.equals(expected):
            return True

        # Integer promotion
        if pa.types.is_integer(actual) and pa.types.is_integer(expected):
            return UDFFlightServer.integer_bit_width(actual) <= UDFFlightServer.integer_bit_width(expected)

        # Float promotion
        if pa.types.is_floating(actual) and pa.types.is_floating(expected):
            return UDFFlightServer.floating_bit_width(actual) <= UDFFlightServer.floating_bit_width(expected)

        # String types
        if pa.types.is_string(expected) and (pa.types.is_string(actual) or pa.types.is_large_string(actual)):
            return True
        if pa.types.is_large_string(expected) and pa.types.is_string(actual):
            return True

        # Timestamps, dates, booleans
        if pa.types.is_timestamp(actual) and pa.types.is_timestamp(expected):
            return True
        if pa.types.is_date32(actual) and pa.types.is_date32(expected):
            return True
        if pa.types.is_date64(actual) and pa.types.is_date64(expected):
            return True
        if pa.types.is_boolean(actual) and pa.types.is_boolean(expected):
            return True
        if pa.types.is_null(actual):
            return True

        return False

    @staticmethod
    def integer_bit_width(t: pa.DataType) -> int:
        if pa.types.is_int8(t): return 8
        if pa.types.is_int16(t): return 16
        if pa.types.is_int32(t): return 32
        if pa.types.is_int64(t): return 64
        return 0

    @staticmethod
    def floating_bit_width(t: pa.DataType) -> int:
        if pa.types.is_float32(t): return 32
        if pa.types.is_float64(t): return 64
        return 0

    def do_exchange(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.MetadataRecordBatchWriter,
    ) -> None:
        """Handles bidirectional streaming UDF execution."""
        logging.info(f"Received exchange request for UDF: {descriptor}")

        python_udf_meta = UDFFlightServer.parse_python_udf_meta(descriptor)
        if not python_udf_meta:
            raise ValueError("Invalid or missing UDF metadata in descriptor")

        logging.info(f"Parsed UDF metadata: {python_udf_meta}")
        loader = UDFLoaderFactory.get_loader(python_udf_meta)
        udf = loader.load()
        logging.info(f"Loaded UDF: {udf}")

        started = False
        for chunk in reader:
            if not chunk.data:
                logging.info("Empty chunk received, skipping")
                continue

            is_compatible, error_msg = UDFFlightServer.check_schema_compatibility(chunk.data, python_udf_meta.input_types)
            if not is_compatible:
                logging.error(f"Schema mismatch: {error_msg}")
                raise ValueError(f"Schema mismatch: {error_msg}")

            result_array = udf(chunk.data)
            logging.info(f"UDF execution result: {result_array}")

            if not python_udf_meta.output_type.equals(result_array.type):
                err = f"Output type mismatch: got {result_array.type}, expected {python_udf_meta.output_type}"
                logging.error(err)
                raise ValueError(err)

            result_batch = pa.RecordBatch.from_arrays([result_array], ["result"])
            if not started:
                writer.begin(result_batch.schema)
                started = True
            writer.write_batch(result_batch)


def check_unix_socket_path(unix_socket_path: str) -> bool:
    """Validates the Unix domain socket path format."""
    if not unix_socket_path:
        logging.error("Unix socket path is empty")
        return False

    if not unix_socket_path.startswith("grpc+unix://"):
        raise ValueError("gRPC UDS URL must start with 'grpc+unix://'")

    socket_path = unix_socket_path[len("grpc+unix://"):].strip()
    if not socket_path:
        logging.error("Extracted socket path is empty")
        return False

    return True


def main(unix_socket_path: str) -> None:
    try:
        if not check_unix_socket_path(unix_socket_path):
            print(f"ERROR: Invalid socket path: {unix_socket_path}", flush=True)
            sys.exit(1)

        current_pid = os.getpid()
        global UNIX_SOCKET_PATH
        UNIX_SOCKET_PATH = f"{unix_socket_path}_{current_pid}.sock"
        server = UDFFlightServer(UNIX_SOCKET_PATH)
        print("Start python server successfully", flush=True)

        logging.info("############################ PYTHON SERVER STARTED ############################")
        logging.info(f"Python UDF server starting up: {datetime.now()}")
        server.wait()

    except Exception as e:
        print(f"ERROR: Failed to start Python UDF server: {type(e).__name__}: {e}", flush=True)
        tb_lines = traceback.format_exception(type(e), e, e.__traceback__)
        if len(tb_lines) > 1:
            print(f"DETAIL: {tb_lines[-2].strip()}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an Arrow Flight UDF server over Unix socket.")
    parser.add_argument("unix_socket_path", type=str, help="Path to the Unix socket (e.g., grpc+unix:///path/to/socket)")
    args = parser.parse_args()
    main(args.unix_socket_path)