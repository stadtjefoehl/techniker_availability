"""
Stadtjeföhl Auftritte – Streamlit + Google Sheets + SQLite

Benötigte Secrets (lokal in .streamlit/secrets.toml oder in Streamlit Cloud → Settings → Secrets):
- GSPREAD_SERVICE_ACCOUNT = kompletter JSON-Inhalt des Service-Account-Keys
- GSHEET_ID = ID des Google Sheets (Teil der URL zwischen /d/ und /edit)
"""

from __future__ import annotations
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

APP_TITLE = "🎵 Stadtjeföhl Auftritte – Techniker Verfügbarkeiten"
DB_PATH = Path("gigs.db")
SHEET_NAME = "geplant 2526"  # Tabellenblatt-Name im Google Sheet
PRIMARY = "#ff2b95"          # Magenta Akzentfarbe

# -------------------------------------------------
# Google Sheets: Auth
# -------------------------------------------------
def get_gspread_client():
    # robust gegenüber '''/""" in Secrets und \n im private_key
    raw = st.secrets["GSPREAD_SERVICE_ACCOUNT"]
    if isinstance(raw, str):
        info = json.loads(raw)
    else:
        info = dict(raw)
    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

# -------------------------------------------------
# SQLite: Verfügbarkeiten (persistente App-DB)
# -------------------------------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS availability (
                event_id TEXT NOT NULL,
                tech_name TEXT NOT NULL,
                status   TEXT NOT NULL CHECK(status IN ('Kann','Kann nicht','Unsicher')),
                updated_at TEXT NOT NULL,
                UNIQUE(event_id, tech_name)
            );
            """
        )

def upsert_availability(event_id: str, tech_name: str, status: str):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO availability(event_id, tech_name, status, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(event_id, tech_name) DO UPDATE SET
                status=excluded.status,
                updated_at=excluded.updated_at
            """,
            (event_id, tech_name.strip(), status, ts),
        )

def load_availability() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql_query("SELECT * FROM availability", conn)
    return df

# -------------------------------------------------
# Datenmodell
# -------------------------------------------------
@dataclass
class ColumnMap:
    col_date: str = "Datum"
    col_time: str = "Uhrzeit"
    col_event: str = "Event"
    col_address: str = "Adresse"
    col_venue: str = "Location"
    col_city: str = "Stadt"
    col_duration: str = "Dauer"
    col_comment: str = "Kommentar"   # NEU

@st.cache_data(show_spinner=False)
def read_excel() -> pd.DataFrame:
    """Liest das Tabellenblatt aus dem Google Sheet als DataFrame (Zeile 1 = Header)."""
    gc = get_gspread_client()
    sh = gc.open_by_key(st.secrets["GSHEET_ID"])
    ws = sh.worksheet(SHEET_NAME)
    rows = ws.get_all_records()  # liest ab Zeile 2 (Header = Zeile 1)
    df = pd.DataFrame(rows)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def build_events_df(df: pd.DataFrame, cmap: ColumnMap) -> pd.DataFrame:
    out = pd.DataFrame()
    # Die echte Tabellenzeile merken (Header = 1, Daten beginnen bei 2)
    out["xl_row"] = (df.reset_index(drop=True).index + 2).astype(int)

    out["date"]     = pd.to_datetime(df[cmap.col_date], errors="coerce").dt.date
    out["time"]     = df[cmap.col_time].astype(str)
    out["event"]    = df[cmap.col_event]
    out["address"]  = df[cmap.col_address]
    out["venue"]    = df[cmap.col_venue]
    out["city"]     = df[cmap.col_city]
    out["duration"] = df[cmap.col_duration] if cmap.col_duration in df.columns else ""
    out["comment"]  = df[cmap.col_comment] if cmap.col_comment in df.columns else ""  # NEU

    # stabile event_id (vereinfachte Variante)
    out["event_id"] = (
        out["date"].astype(str)
        + "|" + out["time"]
        + "|" + out["event"]
        + "|" + out["venue"]
        + "|" + out["city"]
    ).apply(lambda x: str(abs(hash(x))))

    out["display_dt"] = out.apply(lambda r: f"{r['date']} {r['time']}", axis=1)
    out = out.sort_values(["date", "time"]).reset_index(drop=True)
    return out

# -------------------------------------------------
# Google Sheets: Schreiben (Techniker-Spalten)
# -------------------------------------------------
def write_status_to_excel(sheet_row_index_1based: int, tech_name: str, status: str) -> None:
    """Schreibt den Status in die Spalte des Technikers (Name = Spaltenkopf) in derselben Zeile, falls vorhanden."""
    gc = get_gspread_client()
    sh = gc.open_by_key(st.secrets["GSHEET_ID"])
    ws = sh.worksheet(SHEET_NAME)

    headers = ws.row_values(1)  # Header-Zeile
    try:
        col_index_1based = [h.strip().lower() for h in headers].index(tech_name.strip().lower()) + 1
    except ValueError:
        # Spalte existiert nicht → freundlich abbrechen
        raise ValueError(f"Spalte '{tech_name}' nicht gefunden. Lege im Sheet eine Spalte mit diesem Namen an.")

    ws.update_cell(sheet_row_index_1based, col_index_1based, status)

# -------------------------------------------------
# App
# -------------------------------------------------
def main():
    st.set_page_config(page_title="Stadtjeföhl – Auftritte", page_icon="🎵", layout="wide")

    # Kleines Header-Bild oben
    HEADER_IMAGE = "https://stadtjefoehl.de/gallery_gen/e89a9bad2aae7e59e5ff2d16af1d1bc4_1932x964_0x0_1932x966_crop.jpg?ts=1754470199"
    st.image(HEADER_IMAGE, width=250)
    st.markdown(f"<h1 style='color:#111; text-align:left; margin-top: 10px;'>{APP_TITLE}</h1>", unsafe_allow_html=True)

    # Styling (doppelte Klammern im CSS!)
    st.markdown(f"""
        <style>
        .stApp {{ background: linear-gradient(180deg, #fafafb 0%, #eef0f7 100%); }}
        .block-container {{ padding-top: 0.75rem; max-width: 1100px; }}

        /* Sidebar schwarz mit Logo */
        [data-testid="stSidebar"] {{
            background-color: #000000;
            padding-top: 1rem;
        }}
        [data-testid="stSidebar"] * {{
            color: #ffffff !important;
        }}
        /* Sidebar Input-Felder */
        [data-testid="stSidebar"] input {{
            background-color: #222 !important;
            color: #fff !important;
            border: 1px solid #555 !important;
            border-radius: 6px !important;
            padding: 6px 10px !important;
        }}
        [data-testid="stSidebar"] input::placeholder {{
            color: #aaa !important;
        }}

        /* Typografie / Überschriften */
        .streamlit-expanderHeader {{ 
            color: #111 !important; 
            font-weight: 800 !important; 
            font-size: 1.15rem !important; 
            letter-spacing: 0.3px; 
        }}

        /* Buttons & Radios */
        .stButton>button {{ 
            background: {PRIMARY}; 
            color:#fff; 
            border:0; 
            border-radius:12px; 
            padding:8px 14px; 
        }}
        .stButton>button:hover {{ filter: brightness(1.06); }}
        .stRadio > div[role="radiogroup"] label {{
            background: #fdf1f7; 
            border:1px solid {PRIMARY}; 
            color:#e0006b; 
            border-radius:999px; 
            padding:6px 12px; 
            margin-right:8px;
        }}

        /* Karten & Tabellen */
        .st-expander {{ 
            background: #ffffff; 
            border: 1px solid #e6e8f0; 
            border-radius: 14px; 
            box-shadow: 0 2px 10px rgba(16,24,40,0.05); 
        }}
        .stDataFrame {{ background: #fff !important; }}
        .stDataFrame thead tr th {{ color: #111 !important; }}
        .stDataFrame tbody tr td {{ color: #333 !important; }}
        </style>
    """, unsafe_allow_html=True)

    init_db()

    # Sidebar: Logo + Name + Refresh
    with st.sidebar:
        st.image("logo.png", width=160)
        st.header("👤 Dein Name")
        tech_name = st.text_input("Name für Eintragung", value="", placeholder="Dein Name")
        if st.button("🔄 Aktualisieren"):
            st.cache_data.clear()
            st.rerun()

    # Events laden
    try:
        cmap = ColumnMap()
        events = build_events_df(read_excel(), cmap)
    except Exception as e:
        st.error(f"Konnte Google Sheet nicht laden: {e}")
        return

    avail = load_availability()

    st.subheader("📅 Termine")
    for idx, row in events.iterrows():
        eid = row["event_id"]
        date_obj = row["date"]
        date_str = date_obj.strftime("%d.%m.%Y") if hasattr(date_obj, "strftime") else str(date_obj)
        title = f"{date_str} — {row['time']} — {row['event']} — {row['venue']} ({row['city']})"

        with st.expander(title, expanded=False):
            # Adresse & Dauer & Kommentar nur im Expander
            if isinstance(row.get("address"), str) and row["address"].strip():
                st.markdown(f"**📍 Adresse:** {row['address']}")
            dur_text = str(row.get("duration") or "").strip()
            if dur_text:
                st.markdown(f"**⏱ Dauer:** {dur_text}")
            cmt_text = str(row.get("comment") or "").strip()
            if cmt_text:
                st.markdown(f"**💬 Kommentar:** {cmt_text}")

            a_df = avail[avail["event_id"] == eid].copy()
            counts = a_df.groupby("status").size().reindex(["Kann","Unsicher","Kann nicht"], fill_value=0)
            st.markdown(f"""
                <div style='display:flex; gap:8px; flex-wrap:wrap; margin:.25rem 0 .5rem;'>
                  <span style='font-weight:600; padding:6px 10px; border-radius:999px; border:1px solid #a7f3d0; background:#ecfdf5; color:#065f46;'>✅ Kann: {counts['Kann']}</span>
                  <span style='font-weight:600; padding:6px 10px; border-radius:999px; border:1px solid #fde68a; background:#fffbeb; color:#92400e;'>❔ Unsicher: {counts['Unsicher']}</span>
                  <span style='font-weight:600; padding:6px 10px; border-radius:999px; border:1px solid #fecaca; background:#fef2f2; color:#991b1b;'>❌ Kann nicht: {counts['Kann nicht']}</span>
                </div>
            """, unsafe_allow_html=True)

            if not a_df.empty:
                a_df = a_df.sort_values("tech_name")[["tech_name", "status", "updated_at"]]
                a_df.rename(columns={"tech_name": "Name", "status": "Status", "updated_at": "Aktualisiert (UTC)"}, inplace=True)
                st.dataframe(a_df, use_container_width=True, hide_index=True)
            else:
                st.caption("Noch keine Einträge.")

            if tech_name:
                status = st.radio(
                    f"Dein Status für {title}",
                    options=["Kann", "Unsicher", "Kann nicht"],
                    horizontal=True,
                    key=f"radio-{eid}-{idx}",
                )
                if st.button("Speichern", key=f"save-{eid}-{idx}"):
                    upsert_availability(eid, tech_name, status)
                    # Versuche ins Google Sheet zu schreiben, wenn eine Spalte mit diesem Namen existiert
                    try:
                        write_status_to_excel(int(row["xl_row"]), tech_name.strip(), status)
                        st.success("Gespeichert (App + Google Sheet).")
                    except ValueError:
                        st.info("Gespeichert in der App. Lege im Sheet eine Spalte mit deinem Namen an, um auch dort zu schreiben.")
                    except Exception as ex:
                        st.warning(f"Gespeichert in der App. Google-Sheet-Update fehlgeschlagen: {ex}")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.warning("Bitte gib links in der Seitenleiste deinen Namen ein.")

if __name__ == "__main__":
    main()
