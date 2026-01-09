# scripts/pipeline.py
from pathlib import Path
import argparse
import json
import pdfplumber
import re
from typing import List, Dict, Optional, Tuple
from collections import Counter
from db import get_conn, insert_debra_swear_bucket

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

DEBRA_ALIASES = {
    "DEBRA",
    "DEB",
    "DEBRA MORGAN",
}

def is_debra(speaker: str) -> bool:
    s = speaker.upper().strip()
    # exact known aliases
    if s in DEBRA_ALIASES:
        return True
    # covers things like "DEBRA (something got merged into speaker)" or "DEBRA MORGAN JR" etc.
    return s.startswith("DEBRA")
    """
    debra_dialogue: list of dicts like {"speaker":"DEBRA","text":"...","mode":"OS"}
    Returns counts by bucket. FUCK* counts any word token containing 'fuck' anywhere.
    """
    text = "\n".join(d.get("text", "") for d in debra_dialogue).lower()
    text = text.replace("’", "'").replace("`", "'").replace("Õ", "'")

    tokens = re.findall(r"[a-z']+", text)

    buckets = Counter()
    buckets["FUCK*"] = sum(1 for tok in tokens if "fuck" in tok)

    # keep a few other buckets (edit as you like)
    buckets["SHIT*"] = sum(1 for tok in tokens if "shit" in tok)
    buckets["BITCH*"] = sum(1 for tok in tokens if "bitch" in tok)
    buckets["HELL"] = sum(1 for tok in tokens if tok == "hell")
    buckets["DAMN"] = sum(1 for tok in tokens if tok == "damn")
    buckets["DICK*"] = sum(1 for tok in tokens if "dick" in tok)

    # drop zeros
    return Counter({k: v for k, v in buckets.items() if v > 0})

def debra_stats(debra_dialogue):
    """
    Returns:
      total_words: int
      swear_buckets: dict bucket -> [count, words_with_duplicates_in_order]
      total_swear_words: int
      swear_pct: float
    """
    text = "\n".join(d.get("text", "") for d in debra_dialogue).lower()
    text = text.replace("’", "'").replace("`", "'").replace("Õ", "'")

    tokens = re.findall(r"[a-z']+", text)
    total_words = len(tokens)

    def bucket_contains(substr: str):
        words = [t for t in tokens if substr in t]
        return [len(words), words]  # duplicates preserved

    def bucket_exact(word: str):
        words = [t for t in tokens if t == word]
        return [len(words), words]

    swear_buckets = {
        "FUCK*": bucket_contains("fuck"),
        "SHIT*": bucket_contains("shit"),
        "BITCH*": bucket_contains("bitch"),
        "DICK*": bucket_contains("dick"),
        "HELL": bucket_exact("hell"),
        "DAMN": bucket_exact("damn"),
    }

    # drop empty buckets
    swear_buckets = {k: v for k, v in swear_buckets.items() if v[0] > 0}

    total_swear_words = sum(v[0] for v in swear_buckets.values())
    swear_pct = (total_swear_words / total_words * 100) if total_words else 0.0

    return total_words, swear_buckets, total_swear_words, swear_pct


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", default="output")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(exist_ok=True)

    raw = extract_text(Path(args.input))
    clean = clean_text(raw)
    # clean = clean[500:8000]
    chunks = chunk_text(clean)
    dialogue = extract_dialogue_blocks(clean)
    debra_dialogue = [d for d in dialogue if is_debra(d.get("speaker", ""))]
    total_words, swear_buckets, total_swear_words, swear_pct = debra_stats(debra_dialogue)

    conn = get_conn()

    for bucket, (count, bucket_tokens) in swear_buckets.items():
        insert_debra_swear_bucket(
        conn,
        source_file=args.input,
        bucket=bucket,
        count=count,
        tokens=bucket_tokens,
    )

    conn.close()

    
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
    (out / "05_debra_dialogue.jsonl").write_text(
    "\n".join(json.dumps(d, ensure_ascii=False) for d in debra_dialogue),
    encoding="utf-8"
    )
    (out / "06_debra_swear_array.json").write_text(json.dumps(swear_buckets, indent=2, ensure_ascii=False), encoding="utf-8")
    (out / "07_debra_swear_stats.txt").write_text(
        f"Total words: {total_words}\nSwear words: {total_swear_words}\nSwear %: {swear_pct:.2f}%\n",
        encoding="utf-8"
    )

    print(f"✓ extracted {len(chunks)} chunks")
    print(f"✓ extracted {len(dialogue)} dialogue blocks")
    print(f"✓ extracted {len(debra_dialogue)} debra dialogue blocks")

if __name__ == "__main__":
    main()
