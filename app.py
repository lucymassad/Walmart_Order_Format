import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime
import pytz
from io import BytesIO

st.set_page_config(page_title="Walmart Orders Export", layout="wide")
st.title("Walmart Orders Export")
uploaded_file = st.file_uploader("Upload Walmart File (.csv only)", type=["csv"])

OUTPUT_COLUMNS = [
    "PO Number","PO Date","Ship Dates","Must Arrive By","PO Line #","Vendor Style",
    "BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover",
    "Unit of Measure","Unit Price","Number of Inner Packs","PO Total Amount",
    "Promo #","Ticket Description","Other Info / #s","Frt Terms",
    "Buying Party Name","Buying Party Location","Buying Party Address 1","Buying Party Address 2",
    "Buying Party City","Buying Party State","Buying Party Zip","Notes/Comments",
    "Allow/Charge Service","GTIN","Buyers Catalog or Stock Keeping #","UPC/EAN","EDITxnType"
]

DATE_COLS = ["PO Date","Ship Dates","Must Arrive By"]
NUMERIC_TEXT_COLS = ["Qty Ordered","Unit Price","Number of Inner Packs","PO Total Amount","GTIN","UPC/EAN","PO Line #"]

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
    "565378806": ("B1260070","SUNN PALM 6-1-8 4/10#")
}

CASE_SIZE = {
    "665069485": 41, "665113710": 41, "666192291": 51, "665069486": 51,
    "665029761": 75, "665031601": 75, "665029760": 75, "665031679": 75,
    "665031685": 48, "665029764": 48, "665031697": 75, "665029763": 75,
    "665031676": 48, "665029762": 48
}

ORDERS_WIDTHS = {
    "PO Number":16,"PO Date":16,"Ship Dates":16,"Must Arrive By":16,
    "PO Line #":16,"Vendor Style":16,"BC Item#":16,"BC Item Name":60,
    "Qty Ordered":14,"Full Cases":14,"Qty Leftover":14,"Unit of Measure":14,
    "Unit Price":14,"Number of Inner Packs":20,"PO Total Amount":16,"Promo #":18,
    "Ticket Description":16,"Other Info / #s":40,"Frt Terms":30,"Buying Party Name":40,
    "Buying Party Location":30,"Buying Party Address 1":30,"Buying Party Address 2":25,
    "Buying Party City":25,"Buying Party State":25,"Buying Party Zip":25,"Notes/Comments":175,
    "Allow/Charge Service":52,"GTIN":16,"Buyers Catalog or Stock Keeping #":16,"UPC/EAN":16,"EDITxnType":16
}

TRUCKS_COLS = ["Truck","BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover"]
TRUCKS_WIDTHS = {"Truck":16,"BC Item#":16,"BC Item Name":60,"Qty Ordered":14,"Full Cases":14,"Qty Leftover":14}

def _clean_numeric_text(s):
    if pd.isna(s): return pd.NA
    t = str(s).replace(",", "").replace("$", "").strip()
    return t if re.search(r"\d", t) else pd.NA

def _extract_first_date_text(s):
    if pd.isna(s): return pd.NA
    m = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', str(s))
    return m.group(1) if m else s

def _fmt_date_text(s):
    v = _extract_first_date_text(s)
    x = pd.to_datetime(v, errors="coerce")
    return x.strftime("%m/%d/%Y") if pd.notna(x) else pd.NA

def _schema_select(raw):
    name_map = {
        "PO Number":["PO Number","PO #","PONumber","PO"],
        "PO Date":["PO Date","Order Date","PODate"],
        "Ship Dates":["Ship Dates","Ship Date","Delivery Dates","Requested Delivery Date"],
        "Must Arrive By":["Must Arrive By","MABD","MustArriveBy","Must Arrive Date"],
        "PO Line #":["PO Line #","PO Line","Line #","Line Number"],
        "Vendor Style":["Vendor Style","Style"],
        "Qty Ordered":["Qty Ordered","Quantity Ordered","Qty"],
        "Unit of Measure":["Unit of Measure","UOM","Unit"],
        "Unit Price":["Unit Price","Price","UnitPrice","Cost"],
        "Buyers Catalog or Stock Keeping #":["Buyers Catalog or Stock Keeping #","Buyers Catalog #","SKU","Catalog #","Buyer SKU"],
        "UPC/EAN":["UPC/EAN","UPC","EAN","UPC Code"],
        "Number of Inner Packs":["Number of Inner Packs","Inner Packs","Inner Pack Count","InnerPack"],
        "Vendor #":["Vendor #","Vendor Number","Vendor ID","VendorID"],
        "Promo #":["Promo #","Promo Number","Promotion #","Promo"],
        "Ticket Description":["Ticket Description","Ticket Desc","Description","Item Description"],
        "Other Info / #s":["Other Info / #s","Other Info","Other Numbers","Other #s"],
        "Frt Terms":["Frt Terms","Freight Terms","Freight"],
        "Buying Party Name":["Buying Party Name","Buyer Name","Ship To Name","ST Name"],
        "Buying Party Location":["Buying Party Location","Buyer Location","Location #","Location"],
        "Buying Party Address 1":["Buying Party Address 1","Address 1","Addr1","Address1"],
        "Buying Party Address 2":["Buying Party Address 2","Address 2","Addr2","Address2"],
        "Buying Party City":["Buying Party City","City","Town"],
        "Buying Party State":["Buying Party State","State","Province"],
        "Buying Party Zip":["Buying Party Zip","Zip","Postal Code","ZIP Code"],
        "Notes/Comments":["Notes/Comments","Notes","Comments","Comment"],
        "GTIN":["GTIN","GTIN-14"],
        "PO Total Amount":["PO Total Amount","Total Amount","PO Amount","Order Total"],
        "Allow/Charge Service":["Allow/Charge Service","Allowance Service","Charge Service"],
        "EDITxnType":["EDITxnType","EDI Txn Type","Transaction Type"],
        "Record Type":["Record Type","RecordType","Type"]
    }
    norm = {re.sub(r"[^a-z0-9]+","", c.lower()): c for c in raw.columns}
    def find(cands):
        for cand in cands:
            key = re.sub(r"[^a-z0-9]+","", cand.lower())
            if key in norm: return norm[key]
        for cand in cands:
            key = re.sub(r"[^a-z0-9]+","", cand.lower())
            for c in raw.columns:
                if key in re.sub(r"[^a-z0-9]+","", c.lower()): return c
        return None
    sel = pd.DataFrame(index=raw.index)
    for out_col in OUTPUT_COLUMNS + ["Record Type","BC Item#","BC Item Name","Full Cases","Qty Leftover"]:
        if out_col in ["BC Item#","BC Item Name","Full Cases","Qty Leftover"]:
            sel[out_col] = pd.NA
        else:
            src = find(name_map.get(out_col, [out_col]))
            sel[out_col] = raw[src] if src is not None else pd.NA
    for c in DATE_COLS:
        sel[c] = sel[c].apply(_fmt_date_text)
    for c in NUMERIC_TEXT_COLS:
        sel[c] = sel[c].apply(_clean_numeric_text)
    return sel

def _apply_bc_and_cases(sel):
    key = sel["Buyers Catalog or Stock Keeping #"].astype(str).str.replace(r"\.0$","",regex=True).str.strip()
    sel["BC Item#"] = key.map(lambda k: MAP_BC.get(k, (None, None))[0])
    sel["BC Item Name"] = key.map(lambda k: MAP_BC.get(k, (None, None))[1])
    qty = pd.to_numeric(sel["Qty Ordered"], errors="coerce")
    case_sz = key.map(CASE_SIZE)
    full = pd.Series(pd.NA, index=sel.index, dtype="Int64")
    left = pd.Series(pd.NA, index=sel.index, dtype="Int64")
    mask = qty.notna() & case_sz.notna() & (case_sz.astype(float) > 0)
    full[mask] = (qty[mask] // case_sz[mask].astype(int)).astype("Int64")
    left[mask] = (qty[mask] % case_sz[mask].astype(int)).astype("Int64")
    sel["Full Cases"] = full
    sel["Qty Leftover"] = left
    return sel

def _agg_join(s):
    vals = [str(v) for v in s if pd.notna(v) and str(v).strip()!=""]
    if not vals: return pd.NA
    uniq = []
    for v in vals:
        if v not in uniq: uniq.append(v)
    return " | ".join(uniq) if len(uniq)>1 else uniq[0]

def _consolidate(sel):
    po_agg = sel.groupby("PO Number", dropna=False).agg(_agg_join)
    lines = sel[sel["PO Line #"].notna() & (sel["PO Line #"].astype(str).str.strip()!="")]
    line_agg = lines.groupby(["PO Number","PO Line #"], dropna=False).agg(_agg_join).reset_index()
    for c in OUTPUT_COLUMNS:
        if c in ["PO Number","PO Line #","Full Cases","Qty Leftover"]: continue
        mask = line_agg[c].isna() | (line_agg[c].astype(str).str.strip()=="")
        line_agg.loc[mask, c] = line_agg.loc[mask, "PO Number"].map(po_agg[c])
    for c in OUTPUT_COLUMNS:
        if c not in line_agg.columns: line_agg[c] = pd.NA
    line_agg["__sort"] = pd.to_datetime(line_agg["PO Date"], errors="coerce")
    line_agg = line_agg.sort_values("__sort").drop(columns="__sort")
    return line_agg[OUTPUT_COLUMNS]

def _truck_parse_id(notes):
    if not isinstance(notes, str): return None
    m = re.search(r'TRUCK#\s*\S+\s*FOR\s*([0-9A-Za-z]+)', notes)
    return m.group(1) if m else None

def _truck_frames(orders):
    rows_with, rows_missing = [], []
    for _, r in orders.iterrows():
        t = _truck_parse_id(str(r.get("Notes/Comments") or ""))
        rec = {
            "Buyers Catalog or Stock Keeping #": r["Buyers Catalog or Stock Keeping #"],
            "Qty Ordered": pd.to_numeric(r["Qty Ordered"], errors="coerce") or 0
        }
        if t:
            rec["Truck"] = t; rows_with.append(rec)
        else:
            rows_missing.append(rec)

    trucks_df = pd.DataFrame(rows_with) if rows_with else pd.DataFrame(columns=["Truck","Buyers Catalog or Stock Keeping #","Qty Ordered"])
    missing_df = pd.DataFrame(rows_missing) if rows_missing else pd.DataFrame(columns=["Buyers Catalog or Stock Keeping #","Qty Ordered"])

    if not trucks_df.empty:
        trucks_df = trucks_df.groupby(["Truck","Buyers Catalog or Stock Keeping #"], dropna=False)["Qty Ordered"].sum().reset_index()
        trucks_df["BC Item#"] = trucks_df["Buyers Catalog or Stock Keeping #"].map(lambda k: MAP_BC.get(str(k), (None, None))[0])
        trucks_df["BC Item Name"] = trucks_df["Buyers Catalog or Stock Keeping #"].map(lambda k: MAP_BC.get(str(k), (None, None))[1])
        case_sz = trucks_df["Buyers Catalog or Stock Keeping #"].map(CASE_SIZE)
        qty = pd.to_numeric(trucks_df["Qty Ordered"], errors="coerce")
        full = pd.Series(pd.NA, index=trucks_df.index, dtype="Int64")
        left = pd.Series(pd.NA, index=trucks_df.index, dtype="Int64")
        mask = qty.notna() & case_sz.notna()
        full[mask] = (qty[mask] // case_sz[mask].astype(int)).astype("Int64")
        left[mask] = (qty[mask] % case_sz[mask].astype(int)).astype("Int64")
        trucks_df["Full Cases"] = full
        trucks_df["Qty Leftover"] = left
        trucks_df = trucks_df[TRUCKS_COLS]
        trucks_df = trucks_df.sort_values(["Truck","BC Item#"], kind="mergesort")

    if not missing_df.empty:
        missing_df = missing_df.groupby(["Buyers Catalog or Stock Keeping #"], dropna=False)["Qty Ordered"].sum().reset_index()
        missing_df["BC Item#"] = missing_df["Buyers Catalog or Stock Keeping #"].map(lambda k: MAP_BC.get(str(k), (None, None))[0])
        missing_df["BC Item Name"] = missing_df["Buyers Catalog or Stock Keeping #"].map(lambda k: MAP_BC.get(str(k), (None, None))[1])
        case_sz = missing_df["Buyers Catalog or Stock Keeping #"].map(CASE_SIZE)
        qty = pd.to_numeric(missing_df["Qty Ordered"], errors="coerce")
        full = pd.Series(pd.NA, index=missing_df.index, dtype="Int64")
        left = pd.Series(pd.NA, index=missing_df.index, dtype="Int64")
        mask = qty.notna() & case_sz.notna()
        full[mask] = (qty[mask] // case_sz[mask].astype(int)).astype("Int64")
        left[mask] = (qty[mask] % case_sz[mask].astype(int)).astype("Int64")
        missing_df["Full Cases"] = full
        missing_df["Qty Leftover"] = left
        missing_df = missing_df[["BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover"]]
        missing_df = missing_df.sort_values(["BC Item#"], kind="mergesort")

    return trucks_df, missing_df

def _write_truck_sheet_xlsx(writer, trucks_df, missing_df):
    wb = writer.book
    ws = wb.add_worksheet("Trucks")
    writer.sheets["Trucks"] = ws

    fmt_left = wb.add_format({"align": "left"})
    fmt_bold_left = wb.add_format({"align": "left", "bold": True})

    row = 0
    if not trucks_df.empty:
        for truck_id, grp in trucks_df.groupby("Truck"):
            ws.write(row, 0, "Truck:", fmt_bold_left)
            ws.write(row, 1, str(truck_id), fmt_bold_left)
            row += 1
            headers = ["BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover"]
            for col, h in enumerate(headers):
                ws.write(row, col, h, fmt_bold_left)
            row += 1
            for _, r in grp.iterrows():
                ws.write(row, 0, r.get("BC Item#", ""), fmt_left)
                ws.write(row, 1, r.get("BC Item Name", ""), fmt_left)
                ws.write(row, 2, int(r["Qty Ordered"]) if pd.notna(r["Qty Ordered"]) else "", fmt_left)
                ws.write(row, 3, int(r["Full Cases"]) if pd.notna(r["Full Cases"]) else "", fmt_left)
                ws.write(row, 4, int(r["Qty Leftover"]) if pd.notna(r["Qty Leftover"]) else "", fmt_left)
                row += 1
            row += 1

    ws.write(row, 0, "Missing Truck Information", fmt_bold_left)
    row += 1
    headers = ["BC Item#","BC Item Name","Qty Ordered","Full Cases","Qty Leftover"]
    for col, h in enumerate(headers):
        ws.write(row, col, h, fmt_bold_left)
    row += 1
    if not missing_df.empty:
        for _, r in missing_df.iterrows():
            ws.write(row, 0, r.get("BC Item#", ""), fmt_left)
            ws.write(row, 1, r.get("BC Item Name", ""), fmt_left)
            ws.write(row, 2, int(r["Qty Ordered"]) if pd.notna(r["Qty Ordered"]) else "", fmt_left)
            ws.write(row, 3, int(r["Full Cases"]) if pd.notna(r["Full Cases"]) else "", fmt_left)
            ws.write(row, 4, int(r["Qty Leftover"]) if pd.notna(r["Qty Leftover"]) else "", fmt_left)
            row += 1

    ws.set_column(0, 0, TRUCKS_WIDTHS["Truck"], fmt_left)
    ws.set_column(1, 1, TRUCKS_WIDTHS["BC Item#"], fmt_left)
    ws.set_column(2, 2, TRUCKS_WIDTHS["BC Item Name"], fmt_left)
    ws.set_column(3, 3, TRUCKS_WIDTHS["Qty Ordered"], fmt_left)
    ws.set_column(4, 4, TRUCKS_WIDTHS["Full Cases"], fmt_left)
    ws.set_column(5, 5, TRUCKS_WIDTHS["Qty Leftover"], fmt_left)

if uploaded_file:
    try:
        raw = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        st.stop()

    with st.spinner("Your File is Being Processed..."):
        raw.columns = [c.strip() for c in raw.columns]
        raw = raw.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        sel = _schema_select(raw)
        sel = _apply_bc_and_cases(sel)
        orders = _consolidate(sel)

        numeric_cols = ["Qty Ordered","Unit Price","Number of Inner Packs","PO Total Amount","Full Cases","Qty Leftover"]
        for c in numeric_cols:
            if c in orders.columns:
                orders[c] = pd.to_numeric(orders[c], errors="coerce")

        trucks_df, missing_df = _truck_frames(orders)

    st.success("Done")

    trucks_df, missing_df = _truck_frames(orders)

    tz = pytz.timezone("America/New_York")
    ts = datetime.now(tz).strftime("%m.%d.%Y_%H.%M")
    fname = f"Walmart_Export_{ts}.xlsx"

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Orders sheet
        orders.to_excel(writer, index=False, sheet_name="Orders")
        wb = writer.book
        fmt_left = wb.add_format({"align": "left"})
        ws_orders = writer.sheets["Orders"]
        ws_orders.set_row(0, None, fmt_left)  # header row left
        # set widths per your map
        col_index = {col: i for i, col in enumerate(orders.columns)}
        for col_name, width in ORDERS_WIDTHS.items():
            if col_name in col_index:
                ws_orders.set_column(col_index[col_name], col_index[col_name], width, fmt_left)

        # Trucks sheet (grouped layout + widths)
        _write_truck_sheet_xlsx(writer, trucks_df, missing_df)

    st.download_button(
        "Download Walmart Export (1 File, 2 Sheets)",
        data=output.getvalue(),
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
