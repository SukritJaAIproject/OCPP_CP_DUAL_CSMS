"""Export only reviewed pure CP definitions for the portable simulator package."""
import ast
import copy
import hashlib
from pathlib import Path
from tools.cp_protocol_harness import DEFINITIONS, METHODS, _extract


def export(source, destination):
    text = Path(source).read_text(encoding="utf-8")
    tree = ast.parse(text)
    nodes = [copy.deepcopy(node) for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in DEFINITIONS]
    core = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "ChargePoint")
    methods = [copy.deepcopy(node) for node in core.body
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in METHODS]
    for node in methods:
        node.decorator_list = []
    nodes.append(ast.ClassDef(name="ChargePoint", bases=[], keywords=[], body=methods, decorator_list=[]))
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    result = ("# Pure protocol AST export for the SIMULATOR, not the charger application.\n"
              "# Original CP.py SHA256: " + hashlib.sha256(text.encode("utf-8")).hexdigest() + "\n"
              + ast.unparse(module) + "\n")
    if ast.dump(_extract(text)) != ast.dump(_extract(result)):
        raise ValueError("Exported protocol behavior differs from CP.py")
    names = {node.id for node in ast.walk(ast.parse(result)) if isinstance(node, ast.Name)}
    if names & {"GPIO", "line_notify", "bot_token", "plc", "initialize_plc_runtime"}:
        raise ValueError("A hardware or notification integration entered the export")
    Path(destination).write_text(result, encoding="utf-8")
    return hashlib.sha256(result.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    print(export(root / "CP.py", Path(__file__).with_name("cp_protocol_defs.py")))
