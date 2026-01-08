# scripts/pipeline.py
from pathlib import Path
import argparse
import json
import pdfplumber
import re

def extract_text(pdf_path: Path) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)

def clean_text(t: str) -> str:
    t = t.replace("\ufeff", "")
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def chunk_text(text, max_chars=1200):
    chunks, buf = [], ""
    for para in text.split("\n\n"):
        if len(buf) + len(para) > max_chars:
            chunks.append(buf.strip())
            buf = para
        else:
            buf += "\n\n" + para
    if buf.strip():
        chunks.append(buf.strip())
    return chunks

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", default="output")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(exist_ok=True)

    raw = extract_text(Path(args.input))
    clean = clean_text(raw)
    chunks = chunk_text(clean)

    (out / "01_raw.txt").write_text(raw, encoding="utf-8")
    (out / "02_clean.md").write_text(clean, encoding="utf-8")
    (out / "03_chunks.jsonl").write_text(
        "\n".join(json.dumps(c, ensure_ascii=False) for c in chunks),
        encoding="utf-8"
    )

    print(f"✓ extracted {len(chunks)} chunks")

if __name__ == "__main__":
    main()
