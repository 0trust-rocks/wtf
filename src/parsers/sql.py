import traceback
import ast
import json
import sqlglot
import charset_normalizer
import multiprocessing
import queue  # For the Empty exception
import os

from typing import List
from sqlglot import expressions
from utils.logs import get_logger
from .base_parser import BaseParser

logger = get_logger(__name__)

class SQLParser(BaseParser):
    _EXTENSIONS = ['.sql']
    table_schemas = {}
    encoding = 'utf-8'

    def _preprocess_schemas(self):
        """Standard schema collection logic."""
        logger.info("Starting schema preprocessing pass...")
        try:
            with open(self.file_path, 'rb') as f:
                sample = f.read(20480)
                results = charset_normalizer.from_bytes(sample).best()
                self.encoding = results.encoding if results else 'utf-8'
                logger.info(f"Detected encoding: {self.encoding}")
        except Exception as e:
            logger.warning(f"Detection failed, using utf-8-sig: {e}")
            self.encoding = 'utf-8-sig'

        try:
            with open(self.file_path, 'r', encoding=self.encoding, errors='replace') as f:
                buffer = []
                for line in f:
                    try:
                        stripped = line.strip()
                        if not stripped or stripped.startswith(('--', '/*')):
                            continue
                        buffer.append(line)
                        if stripped.endswith(';'):
                            full_sql = "".join(buffer).strip()
                            if full_sql.upper().startswith("CREATE TABLE"):
                                self.parse_create(full_sql)
                            buffer = []
                    except Exception:
                        traceback.print_exc()
        except Exception as e:
            logger.error(f"Error during schema preprocessing: {e}")

    @staticmethod
    def _file_reader_worker(file_path, encoding, sql_queue, num_threads):
        """Top-level static method to avoid pickling 'self' or local closures."""
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                buffer = []
                for line in f:
                    stripped = line.strip()
                    if not stripped or stripped.startswith(('--', '/*')):
                        continue
                    buffer.append(line)
                    if stripped.endswith(';'):
                        full_sql = "".join(buffer).strip()
                        if full_sql.upper().startswith("INSERT"):
                            sql_queue.put(full_sql)
                        buffer = []
        except Exception as e:
            logger.error(f"File reader failed: {e}")
        finally:
            # Signal all workers to stop
            for _ in range(num_threads):
                sql_queue.put(None)

    @staticmethod
    def _parse_worker(input_q, output_q, schemas):
        """Logic for parsing SQL strings in separate processes."""
        while True:
            sql = input_q.get()
            if sql is None:
                break
            try:
                parsed = sqlglot.parse_one(sql, read="mysql")
                if parsed and isinstance(parsed, expressions.Insert):
                    target = parsed.this
                    table_name = ""
                    columns = []

                    if isinstance(target, expressions.Schema):
                        table_name = target.this.name
                        columns = [col.name for col in target.expressions]
                    elif isinstance(target, expressions.Table):
                        table_name = target.name
                    else:
                        table_name = target.sql()

                    if not columns and table_name in schemas:
                        columns = schemas[table_name]

                    values_node = parsed.expression
                    if values_node and isinstance(values_node, expressions.Values):
                        for value_tuple in values_node.expressions:
                            # Inline value extraction to avoid referencing 'self'
                            raw_values = []
                            for v in value_tuple.expressions:
                                val = None
                                if isinstance(v, expressions.Literal):
                                    val = v.this if v.is_string else (ast.literal_eval(v.this) if v.is_number else v.this)
                                elif isinstance(v, expressions.Null): val = None
                                elif isinstance(v, expressions.Boolean): val = v.this
                                else: val = v.sql()
                                
                                if isinstance(val, str) and (val.startswith('{') or val.startswith('[')):
                                    try:
                                        p_json = json.loads(val)
                                        if isinstance(p_json, (dict, list)): val = p_json
                                    except: pass
                                raw_values.append(val)

                            if columns and len(columns) == len(raw_values):
                                row_dict = dict(zip(columns, raw_values))
                                row_dict["_table"] = table_name
                                output_q.put(row_dict)
            except Exception as e:
                logger.error(f"Worker process error: {e}")

    def get_itr(self):
        self._preprocess_schemas()

        sql_queue = multiprocessing.Queue(maxsize=1000)
        result_queue = multiprocessing.Queue(maxsize=2000)
        
        # 1. Start Workers
        processes = []
        for _ in range(self.num_threads):
            p = multiprocessing.Process(
                target=self._parse_worker, 
                args=(sql_queue, result_queue, self.table_schemas)
            )
            p.start()
            processes.append(p)

        # 2. Start Reader Process (Passing primitives, not the method/self)
        reader_p = multiprocessing.Process(
            target=self._file_reader_worker,
            args=(self.file_path, self.encoding, sql_queue, self.num_threads)
        )
        reader_p.start()

        # 3. Yield results
        while True:
            try:
                yield result_queue.get(timeout=0.1)
            except queue.Empty:
                # Break if reader is done and all workers are finished
                if not reader_p.is_alive() and all(not p.is_alive() for p in processes):
                    if result_queue.empty():
                        break
            except Exception:
                break

        # Cleanup
        reader_p.join()
        for p in processes:
            p.join()

    def parse_create(self, sql):
        """Extracts column order from CREATE TABLE statements."""
        try:
            parsed = sqlglot.parse_one(sql, read="mysql")
            if isinstance(parsed, expressions.Create):
                schema_expr = parsed.this 
                table_name = schema_expr.this.name
                columns = [defn.this.name for defn in schema_expr.expressions if isinstance(defn, expressions.ColumnDef)]
                self.table_schemas[table_name] = columns
                logger.info(f"Learned schema for table: {table_name} ({len(columns)} columns)")
        except Exception as e:
            logger.error(f"Failed to parse CREATE TABLE: {e}")