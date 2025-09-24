import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime
import pytz

st.set_page_config(page_title="Walmart Orders Export", layout="wide")
st.title("Walmart Orders Export")
st.markdown("Upload Walmart CSV file.")
uploaded_file = st.file_uploader("Upload Walmart File (.csv only)", type=["csv"])

def _norm(s):
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())

def _find_col(df, candidates):
    norm_map = {_norm(c): c for c in df.columns}
    for cand in candidates:
        k = _norm(cand)
        if k in norm_map:
            return norm_map[k]
    for c in df.columns:
        if any(_norm(x) in _norm(c) for x in candidates):
            return c
    return None

def _consolidate_group(g, prio_col):
    g = g.sort_values(prio_col, kind="stable")
    out = {}
    for c in g.columns:
        if c == prio_col:
            continue
        vals = [v for v in g[c].tolist() if pd.notna(v) and str(v).strip() != ""]
        if not vals:
            out[c] = np.nan
        else:
            uniq, seen = [], set()
            for v in vals:
                if v not in seen:
                    uniq.append(v)
                    seen.add(v)
            out[c] = uniq[0] if len(uniq) == 1 else " | ".join(uniq)
    return pd.Series(out)

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        st.stop()

    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        s = df[c].astype(str).str.strip()
        df[c] = s.replace({"": np.nan, "nan": np.nan, "None": np.nan})

    po_col = _find_col(df, ["PO Number","PO #","PONumber","PO"])
    line_col = _find_col(df, ["PO Line #","PO Line","Line #","Line Number"])
    rt_col = _find_col(df, ["Record Type","RecordType","Type"])

    if po_col is None:
        st.error("Could not find a PO Number column.")
        st.stop()

    if rt_col is None:
        df["__rt"] = np.nan
        rt_col = "__rt"

    priority = ["H","O","D","C","T","S","A","F"]
    prio_map = {v:i for i,v in enumerate(priority)}
    df["__prio"] = df[rt_col].map(lambda x: prio_map.get(str(x).strip().upper(), len(priority)) if pd.notna(x) else len(priority))

    if line_col is None:
        out_df = df.groupby(po_col, dropna=False, as_index=False).apply(lambda g: _consolidate_group(g, "__prio"))
        if isinstance(out_df.index, pd.MultiIndex):
            out_df = out_df.reset_index(level=0, drop=True).reset_index(drop=True)
        else:
            out_df = out_df.reset_index(drop=True)
        lead = [po_col]
    else:
        out_df = df.groupby([po_col, line_col], dropna=False, as_index=False).apply(lambda g: _consolidate_group(g, "__prio"))
        if isinstance(out_df.index, pd.MultiIndex):
            out_df = out_df.reset_index(level=[0,1], drop=True).reset_index(drop=True)
        else:
            out_df = out_df.reset_index(drop=True)
        lead = [po_col, line_col]

    orig_cols = [c for c in df.columns if c not in ["__prio","__rt"]]
    rest = [c for c in orig_cols if c not in lead]
    cols = [*lead, *rest]
    cols = [c for c in cols if c in out_df.columns] + [c for c in out_df.columns if c not in cols]
    out_df = out_df[cols]

    tz = pytz.timezone("America/New_York")
    ts = datetime.now(tz).strftime("%m.%d.%Y_%H.%M")
    fname = f"Walmart_Export_{ts}.csv"

    csv_bytes = out_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Download Walmart Export (CSV)", data=csv_bytes, file_name=fname, mime="text/csv")
