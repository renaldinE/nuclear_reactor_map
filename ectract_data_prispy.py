#!/usr/bin/env python3
# Extract "REACTOR DETAILS" + "LIFETIME PERFORMANCE" from Angra-1_Brazil.pdf
# Source PDF: :contentReference[oaicite:0]{index=0}

import re
import json
from pathlib import Path

import pdfplumber

PDF_PATH = Path("C:/Users/renald_e/Downloads/Angra-1_Brazil.pdf")
OUT_JSON = Path("angra1_reactor_details_lifetime_performance.json")


def norm(s: str) -> str:
    return " ".join((s or "").replace("\u00a0", " ").split()).strip()


def find_phrase_bbox(words, phrase: str):
    toks = phrase.split()
    n = len(toks)
    lower = [w["text"].lower() for w in words]
    target = [t.lower() for t in toks]

    for i in range(len(words) - n + 1):
        if lower[i : i + n] == target:
            chunk = words[i : i + n]
            x0 = min(w["x0"] for w in chunk)
            x1 = max(w["x1"] for w in chunk)
            top = min(w["top"] for w in chunk)
            bottom = max(w["bottom"] for w in chunk)
            return (x0, top, x1, bottom)
    return None


def text_in_bbox(page, bbox):
    txt = page.within_bbox(bbox).extract_text(x_tolerance=2, y_tolerance=2) or ""
    return norm(txt)


def extract_row_values(page, label_phrases, next_row_top, left_margin=10, right_margin=10):
    words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
    label_boxes = []
    for ph in label_phrases:
        b = find_phrase_bbox(words, ph)
        if not b:
            label_boxes.append((ph, None))
        else:
            label_boxes.append((ph, b))

    present = [(ph, b) for ph, b in label_boxes if b is not None]
    present.sort(key=lambda t: t[1][0])  # by x0

    # derive column x-ranges from label x0 positions
    col_ranges = {}
    if present:
        xs = [b[0] for _, b in present]
        xs_sorted = xs[:]
        xs_sorted.sort()
        boundaries = xs_sorted + [page.width - right_margin]

        for idx, (ph, b) in enumerate(present):
            x0 = b[0] - 2
            x1 = boundaries[idx + 1] - 2
            col_ranges[ph] = (max(left_margin, x0), min(page.width - right_margin, x1), b)

    # extract value under each label within its column bounds
    out = {}
    for ph in label_phrases:
        if ph not in col_ranges:
            continue
        x0, x1, b = col_ranges[ph]
        y0 = b[3] + 2  # label bottom + padding
        y1 = next_row_top - 2
        if y1 <= y0:
            y1 = y0 + 30
        val = text_in_bbox(page, (x0, y0, x1, y1))
        if val:
            out[ph] = val
    return out


def main():
    if not PDF_PATH.exists():
        raise FileNotFoundError(PDF_PATH)

    with pdfplumber.open(PDF_PATH) as pdf:
        page = pdf.pages[0]
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)

        # Anchor rows by finding the "top" of the next row labels
        b_ref = find_phrase_bbox(words, "Reference Unit Power")          # next row after Reactor Type/Model/Owner/Operator
        b_constr = find_phrase_bbox(words, "Construction Start Date")    # next row after capacities row
        b_firstgrid = find_phrase_bbox(words, "First Grid Connection")   # next row after construction/criticality row
        b_lifetime = find_phrase_bbox(words, "LIFETIME PERFORMANCE")     # end of reactor details block

        if not all([b_ref, b_constr, b_firstgrid, b_lifetime]):
            raise RuntimeError("Could not locate key anchors on page 1 (layout may differ).")

        # --- REACTOR DETAILS ---
        reactor_details = {}

        row1_labels = ["Reactor Type", "Model", "Owner", "Operator"]
        reactor_details.update(
            extract_row_values(page, row1_labels, next_row_top=b_ref[1])
        )

        row2_labels = [
            "Reference Unit Power (Net",  # PRIS wraps "(Net Capacity)" across line breaks in extraction
            "Design Net Capacity",
            "Gross Capacity",
            "Thermal Capacity",
        ]
        # Workaround: match full label as it appears in the PDF text-flow
        # Try to map the first label to the proper phrase used in word stream
        # (it is often split as: Reference Unit Power (Net Capacity))
        # We'll attempt multiple variants.
        if "Reference Unit Power (Net" not in reactor_details:
            pass

        row2_vals = extract_row_values(page, row2_labels, next_row_top=b_constr[1])
        # normalize the key for the first capacity label
        if "Reference Unit Power (Net" in row2_vals:
            row2_vals["Reference Unit Power (Net Capacity)"] = row2_vals.pop("Reference Unit Power (Net")
        reactor_details.update(row2_vals)

        row3_labels = ["Construction Start Date", "First Criticality Date"]
        reactor_details.update(
            extract_row_values(page, row3_labels, next_row_top=b_firstgrid[1])
        )

        row4_labels = ["First Grid Connection", "Commercial Operation Date"]
        reactor_details.update(
            extract_row_values(page, row4_labels, next_row_top=b_lifetime[1])
        )

        # --- LIFETIME PERFORMANCE ---
        b_elec = find_phrase_bbox(words, "Electricity Supplied")
        b_oper_hist = find_phrase_bbox(words, "OPERATING HISTORY")

        if not all([b_elec, b_oper_hist]):
            raise RuntimeError("Could not locate LIFETIME PERFORMANCE anchors on page 1.")

        lifetime = {}
        lp_labels = [
            "Electricity Supplied",
            "Energy Availability Factor",
            "Operation Factor",
            "Energy Unavailability Factor",
            "Load Factor",
        ]
        lifetime.update(
            extract_row_values(page, lp_labels, next_row_top=b_oper_hist[1])
        )

        # extract "Lifetime performance calculated up to year ####"
        lp_footer_box = (10, b_elec[1], page.width - 10, b_oper_hist[1])
        lp_footer_text = text_in_bbox(page, lp_footer_box)
        # pull year if present
        for token in lp_footer_text.split():
            if token.isdigit() and len(token) == 4:
                lifetime["Lifetime performance calculated up to year"] = token
                break

    out = {
        "reactor_details": {k: v for k, v in reactor_details.items() if v},
        "lifetime_performance": {k: v for k, v in lifetime.items() if v},
    }

    OUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_JSON.resolve()}")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()