import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime
import pytz

st.set_page_config(page_title="Walmart Orders Export", layout="wide")
st.title("Walmart Orders Export")
st.markdown("Upload Walmart CSV file.")
uploaded_file = st.file_uploader("Upload Walmart File (.csv only)", type=["csv"])

OUTPUT_COLUMNS = [
    "PO Number","PO Date","Retailers PO","Ship Dates","Cancel Date","PO Line #",
    "BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover","Unit of Measure","Unit Price",
    "Buyers Catalog or Stock Keeping #","UPC/EAN","Vendor Style","Number of Inner Packs",
    "Vendor #","Promo #","Ticket Description","Other Info / #s","Frt Terms","Payment Terms %",
    "Payment Terms Disc Days Due","Payment Terms Net Days","Allow/Charge Type","Allow/Charge Service",
    "Allow/Charge Amt","Allow/Charge %","Buying Party Name","Buying Party Location",
    "Buying Party Address 1","Buying Party Address 2","Buying Party City","Buying Party State",
    "Buying Party Zip","Buying Party Country","Notes/Comments","GTIN","PO Total Amount",
    "Must Arrive By","EDITxnType"
]

DATE_COLS = ["PO Date","Ship Dates","Cancel Date","Must Arrive By"]
NUMERIC_COLS = ["Qty Ordered","Unit Price","Number of Inner Packs","Payment Terms %","Payment Terms Disc Days Due","Payment Terms Net Days","Allow/Charge Amt","Allow/Charge %","PO Total Amount","GTIN","UPC/EAN","PO Line #"]

MAP_BC = {
    "665069485": ("B8100217","(BURPEE), (ECO FRIEND), SEED STARTING MIX, 0.06-0.03-0.03, 12 QT"),
    "665113710": ("B8100217","(BURPEE), (ECO FRIEND), SEED STARTING MIX, 0.06-0.03-0.03, 12 QT"),
    "666192291": ("B8100925","(BURPEE), (ECO FRIEND), SEED STARTING MIX, 0.06-0.03-0.03, 16 QT"),
    "665069486": ("B8100925","(BURPEE), (ECO FRIEND), SEED STARTING MIX, 0.06-0.03-0.03, 16 QT"),
    "671533940": ("B8100204","(BURPEE), (NAT ORG), ECO FRIENDLY SEED START MIX COIR - BRICK, 8 QT"),
    "665029761": ("B8100914","3-3-2 (EXPERT GARDENER ORG), CHICKEN MANURE, PF, 4 LB."),
    "665031601": ("B8100914","3-3-2 (EXPERT GARDENER ORG), CHICKEN MANURE, PF, 4 LB."),
    "665029760": ("B8100930","3-5-6 (EXPERT GARDENER ORG), VEG TOMATO, PF, 4 LB."),
    "665031679": ("B8100930","3-5-6 (EXPERT GARDENER ORG), VEG TOMATO, PF, 4 LB."),
    "665031685": ("B8100929","3-5-6 (EXPERT GARDENER ORG), VEG TOMATO, PF, 8 LB"),
    "665029764": ("B8100929","3-5-6 (EXPERT GARDENER ORG), VEG TOMATO, PF, 8 LB"),
    "665031697": ("B8100932","4-4-4 (EXPERT GARDENER ORG), ALL PURPOSE, PF, 4 LB."),
    "665029763": ("B8100932","4-4-4 (EXPERT GARDENER ORG), ALL PURPOSE, PF, 4 LB."),
    "665031676": ("B8100928","4-4-4 (EXPERT GARDENER ORG), ALL PURPOSE, PF, 8 LB."),
    "665029762": ("B8100928","4-4-4 (EXPERT GARDENER ORG), ALL PURPOSE, PF, 8 LB."),
    "665106936": ("B1258730","SUNN 6-6-6 33#"),
    "656676067": ("B1201480","SUNN BLOOM 2-10-10 20#"),
    "565380341": ("B1201480","SUNN BLOOM 2-10-10 20#"),
    "565378804": ("B1202370","SUNN CITRUS 6-4-6 4/10#"),
    "656676244": ("B1202380","SUNN CITRUS FERTILIZER 20#"),
    "565380344": ("B1202380","SUNN CITRUS FERTILIZER 20#"),
    "656676073": ("B1224080","SUNN GARD/AZA/CAM 8-4-8 20#"),
    "565380342": ("B1224080","SUNN GARD/AZA/CAM 8-4-8 20#"),
    "565378805": ("B1224070","SUNN GARD/AZA/CAM 8-4-8 4/10#"),
    "665106990": ("B1251580","SUNN NITRO GREEN 16-0-8 33#"),
    "656679467": ("B1260080","SUNN PALM 6-1-8 20 LB"),
    "565380343": ("B1260080","SUNN PALM 6-1-8 20 LB"),
    "565378806": ("B1260070","SUNN PALM 6-1-8 4/10#"),
}

CASE_SIZE = {
    "665069485": 41, "665113710": 41, "666192291": 51, "665069486": 51,
    "665029761": 75, "665031601": 75, "665029760": 75, "665031679": 75,
    "665031685": 48, "665029764": 48, "665031697": 75, "665029763": 75,
    "665031676": 48, "665029762": 48,
}

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

def _clean_numeric(s):
    if pd.isna(s): return np.nan
    t = str(s).replace(",", "").replace("$", "").strip()
    return t if re.search(r"\d", t) else np.nan

def _parse_date_text(s):
    if pd.isna(s) or str(s).strip()=="":
        return np.nan
    x = pd.to_datetime(s, errors="coerce")
    if pd.isna(x):
        return np.nan
    return x.strftime("%m/%d/%Y")

def _select_schema(df):
    out = pd.DataFrame(index=df.index)
    name_map = {
        "PO Number": ["PO Number","PO #","PONumber","PO"],
        "PO Date": ["PO Date","Order Date","PODate"],
        "Retailers PO": ["Retailers PO","Retailer PO","RetailersPO"],
        "Ship Dates": ["Ship Dates","Ship Date"],
        "Cancel Date": ["Cancel Date","CancelDate"],
        "PO Line #": ["PO Line #","PO Line","Line #","Line Number"],
        "Qty Ordered": ["Qty Ordered","Quantity Ordered","Qty"],
        "Unit of Measure": ["Unit of Measure","UOM"],
        "Unit Price": ["Unit Price","Price","UnitPrice"],
        "Buyers Catalog or Stock Keeping #": ["Buyers Catalog or Stock Keeping #","Buyers Catalog #","SKU","Catalog #"],
        "UPC/EAN": ["UPC/EAN","UPC","EAN"],
        "Vendor Style": ["Vendor Style","Style"],
        "Number of Inner Packs": ["Number of Inner Packs","Inner Packs","Inner Pack Count"],
        "Vendor #": ["Vendor #","Vendor Number","Vendor ID"],
        "Promo #": ["Promo #","Promo Number","Promotion #"],
        "Ticket Description": ["Ticket Description","Ticket Desc","Description"],
        "Other Info / #s": ["Other Info / #s","Other Info","Other Numbers"],
        "Frt Terms": ["Frt Terms","Freight Terms"],
        "Payment Terms %": ["Payment Terms %","Payment Terms Percent"],
        "Payment Terms Disc Days Due": ["Payment Terms Disc Days Due","Disc Days Due"],
        "Payment Terms Net Days": ["Payment Terms Net Days","Net Days"],
        "Allow/Charge Type": ["Allow/Charge Type","Allowance Type","Charge Type"],
        "Allow/Charge Service": ["Allow/Charge Service","Allowance Service","Charge Service"],
        "Allow/Charge Amt": ["Allow/Charge Amt","Allowance Amount","Charge Amount","Amount"],
        "Allow/Charge %": ["Allow/Charge %","Allowance %","Charge %","Percent"],
        "Buying Party Name": ["Buying Party Name","Buyer Name"],
        "Buying Party Location": ["Buying Party Location","Buyer Location"],
        "Buying Party Address 1": ["Buying Party Address 1","Address 1"],
        "Buying Party Address 2": ["Buying Party Address 2","Address 2"],
        "Buying Party City": ["Buying Party City","City"],
        "Buying Party State": ["Buying Party State","State"],
        "Buying Party Zip": ["Buying Party Zip","Zip","Postal Code"],
        "Buying Party Country": ["Buying Party Country","Country"],
        "Notes/Comments": ["Notes/Comments","Notes","Comments"],
        "GTIN": ["GTIN"],
        "PO Total Amount": ["PO Total Amount","Total Amount","PO Amount"],
        "Must Arrive By": ["Must Arrive By","MABD","MustArriveBy"],
        "EDITxnType": ["EDITxnType","EDI Txn Type","Transaction Type"],
        "Record Type": ["Record Type","RecordType","Type"]
    }
    for out_col in OUTPUT_COLUMNS + ["Record Type"]:
        if out_col in ["BC Item#","BC Item Name","Full Cases","Qty Leftover"]:
            out[out_col] = pd.NA
            continue
        src = _find_col(df, name_map.get(out_col, [out_col]))
        if src is not None:
            out[out_col] = df[src]
        else:
            out[out_col] = pd.NA
    return out

def _apply_bc_and_cases(sel):
    keycol = "Buyers Catalog or Stock Keeping #"
    if keycol in sel.columns:
        key = sel[keycol].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        sel["BC Item#"] = key.map(lambda k: MAP_BC.get(k, (None, None))[0])
        sel["BC Item Name"] = key.map(lambda k: MAP_BC.get(k, (None, None))[1])
        case_sz = key.map(CASE_SIZE)
    else:
        sel["BC Item#"] = pd.NA
        sel["BC Item Name"] = pd.NA
        case_sz = pd.Series(pd.NA, index=sel.index)
    qty = pd.to_numeric(sel["Qty Ordered"].apply(_clean_numeric), errors="coerce")
    full_cases = pd.Series(pd.NA, index=sel.index, dtype="Int64")
    leftover = pd.Series(pd.NA, index=sel.index, dtype="Int64")
    mask = case_sz.notna() & qty.notna() & (case_sz.astype(float) > 0)
    full_cases[mask] = (qty[mask] // case_sz[mask].astype(int)).astype("Int64")
    leftover[mask] = (qty[mask] % case_sz[mask].astype(int)).astype("Int64")
    sel["Full Cases"] = full_cases
    sel["Qty Leftover"] = leftover
    return sel

def _normalize_types(sel):
    for c in DATE_COLS:
        if c in sel.columns:
            sel[c] = sel[c].apply(_parse_date_text)
    for c in NUMERIC_COLS:
        if c in sel.columns:
            sel[c] = sel[c].apply(_clean_numeric)
    return sel

def _consolidate(sel):
    rt_col = "Record Type" if "Record Type" in sel.columns else None
    prio = {"H":0,"O":1,"D":2,"C":3,"T":4,"S":5,"A":6,"F":7}
    if rt_col is None:
        sel["__prio"] = 99
    else:
        sel["__prio"] = sel[rt_col].map(lambda x: prio.get(str(x).strip().upper(), 99) if pd.notna(x) else 99)
    po_col = "PO Number" if "PO Number" in sel.columns else None
    line_col = "PO Line #" if "PO Line #" in sel.columns else None
    if po_col is None:
        gcols = []
    else:
        gcols = [po_col] + ([line_col] if line_col else [])
    if not gcols:
        g = [sel]
        keys = [None]
    else:
        g = [sub for _, sub in sel.groupby(gcols, dropna=False)]
        keys = [k for k, _ in sel.groupby(gcols, dropna=False)]
    rows = []
    for i, grp in enumerate(g):
        grp = grp.sort_values("__prio", kind="stable")
        out = {}
        for c in OUTPUT_COLUMNS:
            vals = [v for v in grp[c].tolist() if pd.notna(v) and str(v).strip() != ""]
            if not vals:
                out[c] = pd.NA
            else:
                uniq = []
                seen = set()
                for v in vals:
                    if v not in seen:
                        uniq.append(v); seen.add(v)
                out[c] = uniq[0] if len(uniq) == 1 else " | ".join(uniq)
        rows.append(out)
    out_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    return out_df

if uploaded_file:
    try:
        raw = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        st.stop()
    raw.columns = [c.strip() for c in raw.columns]
    for c in raw.columns:
        s = raw[c].astype(str).str.strip()
        raw[c] = s.replace({"": np.nan, "nan": np.nan, "None": np.nan})
    sel = _select_schema(raw)
    sel = _apply_bc_and_cases(sel)
    sel = _normalize_types(sel)
    out_df = _consolidate(sel)
    tz = pytz.timezone("America/New_York")
    ts = datetime.now(tz).strftime("%m.%d.%Y_%H.%M")
    fname = f"Walmart_Export_{ts}.csv"
    csv_bytes = out_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Download Walmart Export (CSV)", data=csv_bytes, file_name=fname, mime="text/csv")
