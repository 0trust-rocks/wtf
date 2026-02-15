import traceback
import ast
import json
import sqlglot
import charset_normalizer

from typing import List, Dict
from sqlglot import expressions
from utils.logs import get_logger
from .base_parser import BaseParser

logger = get_logger(__name__)

class SQLParser(BaseParser):
    _EXTENSIONS = ['.sql']
    table_schemas = {}

    def get_itr(self):
        try:
            with open(self.file_path, 'rb') as f:
                sample = f.read(20480) # Read 20KB for better detection
                results = charset_normalizer.from_bytes(sample).best()
                encoding = results.encoding if results else 'utf-8'
                logger.info(f"Detected encoding: {encoding}")
        except Exception as e:
            logger.warning(f"Detection failed, using utf-8-sig: {e}")
            encoding = 'utf-8-sig'

        with open(self.file_path, 'r', encoding=encoding, errors='replace') as f:
            buffer = []
            for line in f:
                try:
                    stripped = line.strip()
                    if not stripped or stripped.startswith('--') or stripped.startswith('/*'):
                        continue
                        
                    buffer.append(line)
                    if stripped.endswith(';'):
                        full_sql = "".join(buffer).strip()
                        upper_sql = full_sql.upper()
                        
                        if upper_sql.startswith("CREATE TABLE"):
                            self.parse_create(full_sql)
                        elif upper_sql.startswith("INSERT"):
                            yield from self.parse_insert(full_sql)
                        
                        buffer = []
                except Exception:
                    traceback.print_exc()

    def parse_create(self, sql):
        """Extracts column order from CREATE TABLE statements."""
        try:
            parsed = sqlglot.parse_one(sql, read="mysql")
            if isinstance(parsed, expressions.Create):
                schema_expr = parsed.this 
                table_name = schema_expr.this.name
                
                # Extract only column names, ignoring KEY/INDEX definitions
                columns = []
                for defn in schema_expr.expressions:
                    if isinstance(defn, expressions.ColumnDef):
                        columns.append(defn.this.name)
                
                self.table_schemas[table_name] = columns
                logger.info(f"Learned schema for table: {table_name} ({len(columns)} columns)")
        except Exception as e:
            logger.error(f"Failed to parse CREATE TABLE: {e}")

    def parse_insert(self, sql):
        table_name = ""
        try:
            parsed = sqlglot.parse_one(sql, read="mysql")
            if not parsed or not isinstance(parsed, expressions.Insert):
                return

            # Extract Table Name and Columns
            target = parsed.this
            columns: List[str] = []

            if isinstance(target, expressions.Schema):
                table_name = target.this.name
                columns = [col.name for col in target.expressions]
            elif isinstance(target, expressions.Table):
                table_name = target.name
            else:
                table_name = target.sql()

            if not columns and table_name in self.table_schemas:
                columns = self.table_schemas[table_name]

            values_node = parsed.expression
            if values_node and isinstance(values_node, expressions.Values):
                for value_tuple in values_node.expressions:
                    raw_values = [self._extract_value(v) for v in value_tuple.expressions]
                    
                    if columns and len(columns) == len(raw_values):
                        row_dict = dict(zip(columns, raw_values))
                        row_dict["_table"] = table_name
                        yield row_dict
                    else:
                        logger.warning(
                            f"Mismatch in {table_name}: Expected {len(columns)} cols, got {len(raw_values)}"
                        )

        except Exception as e:
            logger.error(f"Error parsing INSERT for {table_name if 'table_name' in locals() else 'unknown'}: {e}")

    def _extract_value(self, item):
        if isinstance(item, expressions.Literal):
            if item.is_string: return self._try_parse_json(item.this)
            if item.is_number:
                try: return ast.literal_eval(item.this)
                except: return item.this
        elif isinstance(item, expressions.Null): return None
        elif isinstance(item, expressions.Boolean): return item.this
        return self._try_parse_json(item.sql())

    def _try_parse_json(self, value):
        if not isinstance(value, str): return value
        stripped = value.strip()
        if not (stripped.startswith('{') or stripped.startswith('[')): return value
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, (dict, list)): return parsed
        except: pass
        return value