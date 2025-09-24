import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import pytz
import importlib

st.set_page_config(page_title="Walmart Orders Export", layout="wide")
st.title("Walmart Orders Export")
st.markdown("Upload SPS order file in original format.")

uploaded_file = st.file_uploader("Upload Walmart File (.csv or .xlsx)", type=["csv", "xlsx"])

def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in df.columns if "date" in c.lower()]
    for c in cols:
        try:
            p = pd.to_datetime(df[c], errors="coerce")
            if p.notna().sum() > 0:
                df[c] = p.dt.strftime("%m/%d/%Y")
        except Exception:
            pass
    return df

def can_import(mod: str) -> bool:
    return importlib.util.find_spec(mod) is not None

if uploaded_file:
    name = uploaded_file.name.lower()

    if name.endswith(".xlsx") and not can_import("openpyxl"):
        st.error("XLSX reading requires openpyxl. Add it to requirements.txt or upload a CSV.")
        st.stop()

    try:
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, dtype=str)
        else:
            df = pd.read_excel(uploaded_file, dtype=str)
    except Exception as e:
        st.error(f"Could not read file: {e}")
        st.stop()

    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        s = df[c].astype(str).str.strip()
        df[c] = s.mask(s.isin(["", "nan", "None"]))

    n = len(df)
    candidates = []
    for c in df.columns:
        nonnull = df[c].notna().sum()
        nunique = df[c].nunique(dropna=True)
        frac = nonnull / max(n, 1)
        if 0.05 <= frac <= 0.8 and nunique <= max(10, int(0.75 * n)):
            candidates.append(c)

    known_meta = [
        "PO Number","PO #","PO Date","Order Date","Requested Delivery Date","Invoice Number","Invoice Date",
        "Ship Date","ASN Date","BOL","BOL#","SCAC","Location #","Buyer Item #","Item#","UPC","UPC/EAN",
        "Department","Dept #","Retailers PO","Ship Dates","Cancel Date","Ship To Location","PO Line #",
        "Qty Ordered","Unit of Measure","Unit Price","Buyers Catalog or Stock Keeping #"
    ]
    for k in known_meta:
        if k in df.columns and k not in candidates:
            candidates.append(k)

    if candidates:
        df[candidates] = df[candidates].ffill()

    df = normalize_dates(df)

    tz = pytz.timezone("America/New_York")
    timestamp = datetime.now(tz).strftime("%m.%d.%Y_%H.%M")
    base = f"Walmart_Export_{timestamp}"

    can_xlsxwriter = can_import("xlsxwriter")
    can_openpyxl = can_import("openpyxl")
    output = BytesIO()

    if can_xlsxwriter or can_openpyxl:
        engine = "xlsxwriter" if can_xlsxwriter else "openpyxl"
        try:
            with pd.ExcelWriter(output, engine=engine) as writer:
                df.to_excel(writer, index=False, sheet_name="Export")
                if engine == "xlsxwriter":
                    workbook = writer.book
                    worksheet = writer.sheets["Export"]
                    header_format = workbook.add_format({"align": "left", "bold": True})
                    for idx, col in enumerate(df.columns):
                        worksheet.set_column(idx, idx, 20)
                        worksheet.write(0, idx, col, header_format)

            st.success("File processed successfully.")
            st.download_button(
                "Download Walmart Export (Excel)",
                data=output.getvalue(),
                file_name=base + ".xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            st.warning(f"Excel writer unavailable ({e}). Providing CSV instead.")
            st.download_button(
                "Download Walmart Export (CSV)",
                data=csv_bytes,
                file_name=base + ".csv",
                mime="text/csv",
            )
    else:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        st.info("Excel engines not installed. Providing CSV download.")
        st.download_button(
            "Download Walmart Export (CSV)",
            data=csv_bytes,
            file_name=base + ".csv",
            mime="text/csv",
        )
