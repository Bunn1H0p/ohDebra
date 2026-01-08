# scripts/pipeline.py
from pathlib import Path
import argparse
import json
import pdfplumber
import re
from typing import List, Dict, Optional, Tuple

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

SPEAKER_RE = re.compile(
    r"""^
    (?P<speaker>[A-Z][A-Z0-9' .-]{1,40})      # DEXTER, DONOVAN, etc.
    (?:\s*\((?P<tag1>[^)]+)\))?              # (V.O.) or (O.S.) or (CONT'D) etc.
    (?:\s*\((?P<tag2>[^)]+)\))?              # optional second (...)
    \s*$
    """,
    re.VERBOSE,
)

# Things that look like ALL CAPS but are NOT dialogue headers
NON_DIALOGUE_HEADERS = (
    "INT.", "EXT.", "CUT TO:", "FADE IN:", "FADE OUT:", "SMASH CUT:", "DISSOLVE TO:",
)

def normalize_tag(tag: str) -> str:
    t = tag.upper().replace("Õ", "'").replace("’", "'").replace("`", "'").strip()
    t = t.replace("CONTÕD", "CONT'D").replace("CONT’D", "CONT'D")
    return t

def mode_from_tags(tag1: Optional[str], tag2: Optional[str]) -> Optional[str]:
    tags = [normalize_tag(t) for t in (tag1, tag2) if t]
    if any("V.O" in t or "VO" == t for t in tags):
        return "VO"
    if any("O.S" in t or "OS" == t for t in tags):
        return "OS"
    return None  # normal on-screen dialogue

def looks_like_speaker_header(line: str) -> Optional[Tuple[str, Optional[str]]]:
    line = line.strip()
    if not line:
        return None
    if any(line.startswith(prefix) for prefix in NON_DIALOGUE_HEADERS):
        return None
    m = SPEAKER_RE.match(line)
    if not m:
        return None

    speaker = m.group("speaker").strip()
    # Filter out obvious non-speakers that are still all-caps action beats
    if speaker in {"PANNING FURTHER DOWN UNTIL", "THE WINDSHIELD AND SEE", "BACK TO SCENE", "NEW ANGLE", "FREEZE FRAME ON HIS FACE"}:
        return None

    mode = mode_from_tags(m.group("tag1"), m.group("tag2"))
    return speaker, mode

def extract_dialogue_blocks(text: str) -> List[Dict[str, str]]:
    """
    Returns list of {speaker, mode, text} for each dialogue block.
    mode is 'VO', 'OS', or omitted/None for normal dialogue.
    """
    lines = text.splitlines()
    results: List[Dict[str, str]] = []

    current_speaker: Optional[str] = None
    current_mode: Optional[str] = None
    buf: List[str] = []

    def flush():
        nonlocal current_speaker, current_mode, buf
        if current_speaker and buf:
            spoken = "\n".join(buf).strip()
            if spoken:
                rec = {"speaker": current_speaker, "text": spoken}
                if current_mode:
                    rec["mode"] = current_mode
                results.append(rec)
        current_speaker, current_mode, buf = None, None, []

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()

        header = looks_like_speaker_header(line)
        if header:
            # starting a new speaker block -> flush previous
            flush()
            current_speaker, current_mode = header
            i += 1

            # Optional: skip a parenthetical line like "(furious)" on its own line
            if i < len(lines):
                nxt = lines[i].strip()
                if re.fullmatch(r"\([^)]*\)", nxt):
                    i += 1
            continue

        # If we are inside a speaker block, keep collecting dialogue lines
        if current_speaker:
            # stop collecting on blank line (common screenplay separator)
            if line.strip() == "":
                flush()
            else:
                buf.append(line)
        i += 1

    flush()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", default="output")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(exist_ok=True)

    raw = extract_text(Path(args.input))
    clean = clean_text(raw)
    clean = clean[500:8000]
    chunks = chunk_text(clean)
    dialogue = extract_dialogue_blocks(clean)

    (out / "01_raw.txt").write_text(raw, encoding="utf-8")
    (out / "02_clean.md").write_text(clean, encoding="utf-8")
    (out / "03_chunks.jsonl").write_text(
        "\n".join(json.dumps(c, ensure_ascii=False) for c in chunks),
        encoding="utf-8"
    )
    (out / "04_dialogue.jsonl").write_text(
    "\n".join(json.dumps(d, ensure_ascii=False) for d in dialogue),
    encoding="utf-8"
    )

    print(f"✓ extracted {len(chunks)} chunks")
    print(f"✓ extracted {len(dialogue)} dialogue blocks")

if __name__ == "__main__":
    main()
