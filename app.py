import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime
import pytz
from io import BytesIO

st.set_page_config(page_title="Walmart Orders Export", layout="wide")
st.title("Walmart Orders Export")
st.markdown("Upload Walmart CSV file.")
uploaded_file = st.file_uploader("Upload Walmart File (.csv only)", type=["csv"])

OUTPUT_COLUMNS = [
    "PO Number","PO Date","Must Arrive By","PO Line #","Vendor Style",
    "BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover",
    "Unit of Measure","Unit Price","Number of Inner Packs","PO Total Amount",
    "Promo #","Ticket Description","Other Info / #s","Frt Terms",
    "Buying Party Name","Buying Party Location","Buying Party Address 1","Buying Party Address 2",
    "Buying Party City","Buying Party State","Buying Party Zip","Notes/Comments",
    "Allow/Charge Service","GTIN","Buyers Catalog or Stock Keeping #","UPC/EAN","EDITxnType"
]

DATE_COLS = ["PO Date","Must Arrive By"]
NUMERIC_TEXT_COLS = ["Qty Ordered","Unit Price","Number of Inner Packs","PO Total Amount","GTIN","UPC/EAN","PO Line #"]

MAP_BC = {
    "665069485": ("B8100217","(BURPEE)...12 QT"),
    "665113710": ("B8100217","(BURPEE)...12 QT"),
    "666192291": ("B8100925","(BURPEE)...16 QT"),
    "665069486": ("B8100925","(BURPEE)...16 QT"),
    "671533940": ("B8100204","(BURPEE)...8 QT"),
    "665029761": ("B8100914","Chicken Manure 4 LB."),
    "665031601": ("B8100914","Chicken Manure 4 LB."),
    "665029760": ("B8100930","Veg Tomato 4 LB."),
    "665031679": ("B8100930","Veg Tomato 4 LB."),
    "665031685": ("B8100929","Veg Tomato 8 LB"),
    "665029764": ("B8100929","Veg Tomato 8 LB"),
    "665031697": ("B8100932","All Purpose 4 LB."),
    "665029763": ("B8100932","All Purpose 4 LB."),
    "665031676": ("B8100928","All Purpose 8 LB."),
    "665029762": ("B8100928","All Purpose 8 LB."),
}
CASE_SIZE = {
    "665069485": 41,"665113710": 41,"666192291": 51,"665069486": 51,
    "665029761": 75,"665031601": 75,"665029760": 75,"665031679": 75,
    "665031685": 48,"665029764": 48,"665031697": 75,"665029763": 75,
    "665031676": 48,"665029762": 48,
}

def _clean_num(s):
    if pd.isna(s): return pd.NA
    s = str(s).replace(",","").replace("$","").strip()
    return s if re.search(r"\d",s) else pd.NA

def _extract_date(s):
    if pd.isna(s): return pd.NA
    m = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', str(s))
    if not m: return pd.NA
    return pd.to_datetime(m.group(1), errors="coerce")

def _apply_bc_cases(df):
    key = df["Buyers Catalog or Stock Keeping #"].astype(str).str.replace(r"\.0$","",regex=True).str.strip()
    df["BC Item#"] = key.map(lambda k: MAP_BC.get(k,(None,None))[0])
    df["BC Item Name"] = key.map(lambda k: MAP_BC.get(k,(None,None))[1])
    qty = pd.to_numeric(df["Qty Ordered"], errors="coerce")
    case_sz = key.map(CASE_SIZE)
    full = pd.Series(pd.NA,index=df.index,dtype="Int64")
    left = pd.Series(pd.NA,index=df.index,dtype="Int64")
    mask = qty.notna() & case_sz.notna()
    full[mask] = (qty[mask] // case_sz[mask].astype(int)).astype("Int64")
    left[mask] = (qty[mask] % case_sz[mask].astype(int)).astype("Int64")
    df["Full Cases"] = full; df["Qty Leftover"] = left
    return df

def _agg_join(series):
    vals=[str(v) for v in series if pd.notna(v) and str(v).strip()!=""]
    if not vals: return pd.NA
    uniq=[]
    for v in vals:
        if v not in uniq: uniq.append(v)
    return " | ".join(uniq) if len(uniq)>1 else uniq[0]

def _consolidate(df):
    po_agg=df.groupby("PO Number",dropna=False).agg(_agg_join)
    lines=df[df["PO Line #"].notna() & (df["PO Line #"].astype(str).str.strip()!="")]
    line_agg=lines.groupby(["PO Number","PO Line #"],dropna=False).agg(_agg_join).reset_index()
    for c in OUTPUT_COLUMNS:
        if c in ["PO Number","PO Line #","Full Cases","Qty Leftover"]: continue
        mask=line_agg[c].isna() | (line_agg[c].astype(str).str.strip()=="")
        line_agg.loc[mask,c]=line_agg.loc[mask,"PO Number"].map(po_agg[c])
    for c in OUTPUT_COLUMNS:
        if c not in line_agg.columns: line_agg[c]=pd.NA
    line_agg["__sort"]=pd.to_datetime(line_agg["PO Date"],errors="coerce")
    line_agg=line_agg.sort_values("__sort").drop(columns="__sort")
    return line_agg[OUTPUT_COLUMNS]

def _truck_summary(orders):
    trucks=[]
    for idx,row in orders.iterrows():
        notes=str(row.get("Notes/Comments") or "")
        m=re.search(r'TRUCK#\s*([0-9A-Za-z]+)',notes)
        if m:
            truck=m.group(1)
            trucks.append({
                "Truck":truck,
                "BC Item#":row["BC Item#"],
                "BC Item Name":row["BC Item Name"],
                "Qty Ordered":pd.to_numeric(row["Qty Ordered"],errors="coerce") or 0,
                "Buyers Catalog or Stock Keeping #":row["Buyers Catalog or Stock Keeping #"]
            })
    if not trucks: return pd.DataFrame()
    tdf=pd.DataFrame(trucks)
    tdf=tdf.groupby(["Truck","BC Item#","BC Item Name","Buyers Catalog or Stock Keeping #"],dropna=False)["Qty Ordered"].sum().reset_index()
    tdf=_apply_bc_cases(tdf)
    return tdf[["Truck","BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover"]]

if uploaded_file:
    raw=pd.read_csv(uploaded_file,dtype=str,keep_default_na=False).replace({"":pd.NA,"nan":pd.NA,"None":pd.NA})
    # keep as-is; assume already mapped/clean
    raw= _apply_bc_cases(raw)
    orders=_consolidate(raw)
    trucks=_truck_summary(orders)

    tz=pytz.timezone("America/New_York")
    ts=datetime.now(tz).strftime("%m.%d.%Y_%H.%M")
    fname=f"Walmart_Export_{ts}.xlsx"

    output=BytesIO()
    with pd.ExcelWriter(output,engine="openpyxl") as writer:
        orders.to_excel(writer,index=False,sheet_name="Orders")
        if not trucks.empty:
            trucks.to_excel(writer,index=False,sheet_name="Trucks")

    st.download_button(
        "Download Walmart Export (Excel)",
        data=output.getvalue(),
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
