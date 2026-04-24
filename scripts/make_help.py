"""Imprime help auto-documentado do Makefile (substitui awk, funciona em Windows/Linux)."""
import re
import sys
from pathlib import Path

makefile = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("Makefile")
text = makefile.read_text(encoding="utf-8")

print(f"\nUso: make <alvo> [VAR=valor ...]\n")

current_section = None
for line in text.splitlines():
    section = re.match(r"^##@\s*(.*)", line)
    if section:
        current_section = section.group(1)
        print(f"\n{current_section}")
        continue
    target = re.match(r"^([a-zA-Z0-9_.-]+):.*##\s*(.*)", line)
    if target:
        print(f"  {target.group(1):<22} {target.group(2)}")
