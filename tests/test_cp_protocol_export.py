import ast
from pathlib import Path
from tools.export_cp_protocol_defs import export
from tools.cp_protocol_harness import SOURCE, _extract


def test_portable_protocol_export_matches_actual_CP_and_excludes_its_startup(tmp_path):
    target = tmp_path / "cp_protocol_defs.py"
    digest = export(SOURCE, target)
    assert len(digest) == 64
    text = target.read_text(encoding="utf-8")
    assert ast.dump(_extract(text)) == ast.dump(_extract(SOURCE.read_text(encoding="utf-8")))
    tree = ast.parse(text)
    assert all(isinstance(node, (ast.FunctionDef, ast.ClassDef)) for node in tree.body)
    assert "initialize_plc_runtime" not in text and "bot_token" not in text
