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
    "Buying Party Name","Buying Party Location","Buying Party Address 1","Buying Party Address 2",
    "Buying Party City","Buying Party State","Buying Party Zip","Buying Party Country",
    "Notes/Comments","GTIN","PO Total Amount","Must Arrive By","EDITxnType"
]

DATE_COLS = ["PO Date","Ship Dates","Cancel Date","Must Arrive By"]
NUMERIC_TEXT_COLS = [
    "Qty Ordered","Unit Price","Number of Inner Packs","Payment Terms %",
    "Payment Terms Disc Days Due","Payment Terms Net Days","PO Total Amount","GTIN","UPC/EAN","PO Line #"
]

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

def _norm(s): return re.sub(r"[^a-z0-9]+", "", str(s).lower())

def _find_col(df, candidates):
    norm_map = {_norm(c): c for c in df.columns}
    for cand in candidates:
        k = _norm(cand)
        if k in norm_map: return norm_map[k]
    for c in df.columns:
        if any(_norm(x) in _norm(c) for x in candidates): return c
    return None

def _clean_numeric_text(s):
    if pd.isna(s): return pd.NA
    t = str(s).replace(",", "").replace("$", "").strip()
    return t if re.search(r"\d", t) else pd.NA

def _extract_first_date(s):
    if pd.isna(s): return pd.NA
    s = str(s)
    m = re.findall(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', s)
    return m[0] if m else s

def _fmt_date_text(s):
    v = _extract_first_date(s)
    x = pd.to_datetime(v, errors="coerce")
    return x.strftime("%m/%d/%Y") if pd.notna(x) else pd.NA

def _schema_select(raw):
    name_map = {
        "PO Number": ["PO Number","PO #","PONumber","PO"],
        "PO Date": ["PO Date","Order Date","PODate"],
        "Retailers PO": ["Retailers PO","Retailer PO","RetailersPO"],
        "Ship Dates": ["Ship Dates","Ship Date","Delivery Dates","Requested Delivery Date"],
        "Cancel Date": ["Cancel Date","CancelDate"],
        "PO Line #": ["PO Line #","PO Line","Line #","Line Number"],
        "Qty Ordered": ["Qty Ordered","Quantity Ordered","Qty"],
        "Unit of Measure": ["Unit of Measure","UOM","Unit"],
        "Unit Price": ["Unit Price","Price","UnitPrice","Cost"],
        "Buyers Catalog or Stock Keeping #": ["Buyers Catalog or Stock Keeping #","Buyers Catalog #","SKU","Catalog #","Buyer SKU"],
        "UPC/EAN": ["UPC/EAN","UPC","EAN","UPC Code"],
        "Vendor Style": ["Vendor Style","Style"],
        "Number of Inner Packs": ["Number of Inner Packs","Inner Packs","Inner Pack Count","InnerPack"],
        "Vendor #": ["Vendor #","Vendor Number","Vendor ID","VendorID"],
        "Promo #": ["Promo #","Promo Number","Promotion #","Promo"],
        "Ticket Description": ["Ticket Description","Ticket Desc","Description","Item Description"],
        "Other Info / #s": ["Other Info / #s","Other Info","Other Numbers","Other #s"],
        "Frt Terms": ["Frt Terms","Freight Terms","Freight"],
        "Payment Terms %": ["Payment Terms %","Payment Terms Percent","Terms %"],
        "Payment Terms Disc Days Due": ["Payment Terms Disc Days Due","Disc Days Due","Discount Days"],
        "Payment Terms Net Days": ["Payment Terms Net Days","Net Days","Terms Net"],
        "Allow/Charge Type": ["Allow/Charge Type","Allowance Type","Charge Type"],
        "Allow/Charge Service": ["Allow/Charge Service","Allowance Service","Charge Service"],
        "Buying Party Name": ["Buying Party Name","Buyer Name","Ship To Name","ST Name"],
        "Buying Party Location": ["Buying Party Location","Buyer Location","Location #","Location"],
        "Buying Party Address 1": ["Buying Party Address 1","Address 1","Addr1","Address1"],
        "Buying Party Address 2": ["Buying Party Address 2","Address 2","Addr2","Address2"],
        "Buying Party City": ["Buying Party City","City","Town"],
        "Buying Party State": ["Buying Party State","State","Province"],
        "Buying Party Zip": ["Buying Party Zip","Zip","Postal Code","ZIP Code"],
        "Buying Party Country": ["Buying Party Country","Country"],
        "Notes/Comments": ["Notes/Comments","Notes","Comments","Comment"],
        "GTIN": ["GTIN","GTIN-14"],
        "PO Total Amount": ["PO Total Amount","Total Amount","PO Amount","Order Total"],
        "Must Arrive By": ["Must Arrive By","MABD","MustArriveBy","Must Arrive Date"],
        "EDITxnType": ["EDITxnType","EDI Txn Type","Transaction Type"],
        "Record Type": ["Record Type","RecordType","Type"]
    }
    sel = pd.DataFrame(index=raw.index)
    for out_col in OUTPUT_COLUMNS + ["Record Type"]:
        if out_col in ["BC Item#","BC Item Name","Full Cases","Qty Leftover"]:
            sel[out_col] = pd.NA
            continue
        src = _find_col(raw, name_map.get(out_col, [out_col]))
        sel[out_col] = raw[src] if src is not None else pd.NA
    for c in DATE_COLS:
        sel[c] = sel[c].apply(_fmt_date_text)
    for c in NUMERIC_TEXT_COLS:
        sel[c] = sel[c].apply(_clean_numeric_text)
    return sel

def _apply_bc_cases(sel):
    key = sel["Buyers Catalog or Stock Keeping #"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    sel["BC Item#"] = key.map(lambda k: MAP_BC.get(k, (None, None))[0])
    sel["BC Item Name"] = key.map(lambda k: MAP_BC.get(k, (None, None))[1])
    case_sz = key.map(CASE_SIZE)
    qty = pd.to_numeric(sel["Qty Ordered"], errors="coerce")
    full_cases = pd.Series(pd.NA, index=sel.index, dtype="Int64")
    leftover = pd.Series(pd.NA, index=sel.index, dtype="Int64")
    mask = case_sz.notna() & qty.notna() & (case_sz.astype(float) > 0)
    full_cases[mask] = (qty[mask] // case_sz[mask].astype(int)).astype("Int64")
    leftover[mask] = (qty[mask] % case_sz[mask].astype(int)).astype("Int64")
    sel["Full Cases"] = full_cases
    sel["Qty Leftover"] = leftover
    return sel

def _agg_join(series):
    vals = [str(v) for v in series if pd.notna(v) and str(v).strip() != ""]
    if not vals: return pd.NA
    uniq, seen = [], set()
    for v in vals:
        if v not in seen:
            uniq.append(v); seen.add(v)
    return uniq[0] if len(uniq) == 1 else " | ".join(uniq)

def _consolidate(sel):
    po_agg = sel.groupby("PO Number", dropna=False).agg(_agg_join)
    line_valid = sel[sel["PO Line #"].notna() & (sel["PO Line #"].astype(str).str.strip()!="")]
    line_agg = line_valid.groupby(["PO Number","PO Line #"], dropna=False).agg(_agg_join).reset_index()
    for c in OUTPUT_COLUMNS:
        if c in ["PO Number","PO Line #","Full Cases","Qty Leftover"]:
            continue
        mask = line_agg[c].isna() | (line_agg[c].astype(str).str.strip()=="")
        line_agg.loc[mask, c] = line_agg.loc[mask, "PO Number"].map(po_agg[c])
    return line_agg[OUTPUT_COLUMNS]

if uploaded_file:
    try:
        raw = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        st.stop()

    raw.columns = [c.strip() for c in raw.columns]
    raw = raw.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    sel = _schema_select(raw)
    sel = _apply_bc_cases(sel)
    out_df = _consolidate(sel)

    tz = pytz.timezone("America/New_York")
    ts = datetime.now(tz).strftime("%m.%d.%Y_%H.%M")
    fname = f"Walmart_Export_{ts}.csv"
    st.download_button("Download Walmart Export (CSV)", data=out_df.to_csv(index=False).encode("utf-8-sig"),
                       file_name=fname, mime="text/csv")
