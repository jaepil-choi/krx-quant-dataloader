from __future__ import annotations

import argparse
import ast
import os
from typing import Any, Dict, List, Optional

import yaml


def parse_core(core_path: str) -> List[Dict[str, Any]]:
    with open(core_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=core_path)

    endpoints: List[Dict[str, Any]] = []

    class EndpointVisitor(ast.NodeVisitor):
        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            bases = [getattr(b, "id", None) or getattr(getattr(b, "attr", None), "id", None) for b in node.bases]
            if "KrxWebIo" not in bases:
                return

            bld_value: Optional[str] = None
            fetch_func: Optional[ast.FunctionDef] = None
            root_key: Optional[str] = None

            for body_item in node.body:
                if isinstance(body_item, ast.FunctionDef) and body_item.name == "bld":
                    # find return string constant
                    for stmt in ast.walk(body_item):
                        if isinstance(stmt, ast.Return):
                            if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
                                bld_value = stmt.value.value
                                break
                if isinstance(body_item, ast.FunctionDef) and body_item.name == "fetch":
                    fetch_func = body_item
                    # try to get root key from return
                    for stmt in ast.walk(body_item):
                        if isinstance(stmt, ast.Subscript):
                            # look for result['...']
                            if isinstance(stmt.slice, ast.Index):  # type: ignore[attr-defined]
                                s = stmt.slice.value
                            else:
                                s = getattr(stmt.slice, "value", None)
                            if isinstance(s, ast.Constant) and isinstance(s.value, str):
                                if s.value in {"output", "OutBlock_1", "block1"}:
                                    root_key = s.value
                                    break

            if not bld_value or not fetch_func:
                return

            # Params from fetch signature (exclude self)
            params = [a.arg for a in fetch_func.args.args if a.arg != "self"]
            params_map: Dict[str, Any] = {}
            for p in params:
                entry: Dict[str, Any] = {"required": True}
                if p in {"strtDd", "startDd"}:
                    entry["type"] = "date(yyyymmdd)"
                    entry["role"] = "start_date"
                elif p in {"endDd"}:
                    entry["type"] = "date(yyyymmdd)"
                    entry["role"] = "end_date"
                elif p in {"trdDd"}:
                    entry["type"] = "date(yyyymmdd)"
                elif p in {"mktId"}:
                    entry["type"] = "enum"
                    entry["enum"] = ["STK", "KSQ", "KNX", "ALL"]
                elif p in {"adjStkPrc"}:
                    entry["type"] = "enum"
                    entry["enum"] = [1, 2]
                    entry["default"] = 1
                else:
                    entry["type"] = "string"
                params_map[p] = entry

            client_policy: Dict[str, Any] = {}
            if "strtDd" in params_map and "endDd" in params_map:
                client_policy = {
                    "chunking": {"days": 730, "gap_days": 1}
                }

            # Build endpoint id from bld
            # e.g., dbms/MDC/STAT/standard/MDCSTAT01602 -> krx.market.MDCSTAT01602
            bld_leaf = bld_value.split("/")[-1]
            endpoint_id = f"krx.market.{bld_leaf}"

            endpoint = {
                "id": endpoint_id,
                "host": "krx",
                "method": "POST",
                "path": "/comm/bldAttendant/getJsonData.cmd",
                "bld": bld_value,
                "params": params_map,
                "client_policy": client_policy or None,
                "response": {
                    "root_keys": [rk for rk in [root_key, "output", "OutBlock_1", "block1"] if rk],
                    "merge": "append",
                    "order_by": "TRD_DD" if "TRD_DD" in {"TRD_DD"} else None,
                },
            }
            endpoints.append(endpoint)

    EndpointVisitor().visit(tree)
    return endpoints


def merge_into_yaml(target_yaml: str, endpoints: List[Dict[str, Any]]) -> None:
    with open(target_yaml, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if "endpoints" not in data:
        data["endpoints"] = {}

    existing_keys = set(data["endpoints"].keys())
    for ep in endpoints:
        ep_id = ep.pop("id")
        if ep_id in existing_keys:
            continue
        # clean None values
        if ep.get("client_policy") is None:
            ep.pop("client_policy", None)
        data["endpoints"][ep_id] = ep

    with open(target_yaml, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--core-path", required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()

    endpoints = parse_core(args.core_path)
    merge_into_yaml(args.target, endpoints)
    print(f"Appended/merged {len(endpoints)} endpoint specs into {args.target}")


if __name__ == "__main__":
    main()


