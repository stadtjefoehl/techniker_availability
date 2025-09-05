"""
Stadtjeföhl Auftritte – Streamlit + Google Sheets + SQLite

Benötigte Secrets (lokal in .streamlit/secrets.toml oder in Streamlit Cloud → Settings → Secrets):
- GSPREAD_SERVICE_ACCOUNT = "type": "service_account",
  "project_id": "stadtjefoehl",
  "private_key_id": "aa4a8b7e8fad97c1b50548bcc948b5ae78b3f29c",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDGVAoDoCgIIhtt\nfy56qVx7uz1/SaIIFYVa2XKB3n/fpdF53PVaDYfS8ECMklKoTGfzJicO1ufxThSC\noonz1qZJ744wJhrwrFZaRtT3moXSPsSRcQhQXgSX3tgDcqJUZAeaYVjNTrqOGcYZ\nvL7lZXoQ+Is0xGzB4P5lBYkHJWpEymqraNuYEGPFAXKLybgpMDFz/T7C/l1dwltW\n1Qn/MnroDQp+0U/1vfvlIbjuWVSSMRIm2JyZb4lRAjQir8WeuHVZJmGNy3kazloF\nOKzdktlo4GMi7XN4X0X4feUz6chzBqv+DiNZcr9K6Wh32dl+wbr+1DwWm1HZ/Ym2\nWYUmdr6/AgMBAAECggEAB0T6Tv1g/5dRyQwshct3u9ig8u2d1k0E+SUCQ6IGWYoS\n6Qlr0X6wd/oIFlRLWlGJTTTHTA3ILPnvL4X0p4Q6eSL3jmqUeeDfPhTdkSvP9Dpf\nwHL+WoUaXFS14eki676fjA2TEIduYsStabEXkCyi3O9h7tpfoCWhPXkGokVBUBRz\naE5wx7g29vm59TVlwAF0EwIMiS7oFAElM8IUWOk93pnpBbvi1/VA9epbC23TQNHl\nyPMZ835B8Qpb+C/JLxkbIYxi3wljk/eeQdVekpkRlgXBz2Xo8EVBiZUm3DQxJTYm\njHDd05WHAkhCsgfOVTEgsQjA9V+PsQJ6QN9nmcDA7QKBgQDvc+fiSheEcEkKq2Wx\nC4PyC0iLYzmA7fHLdbCq0H/4X6vS+3nIJXYJfj4HTd50G6TrfF8TRRLdEi7E80gP\nSp2pijlBGY8JeY9uZNhYRFgPtQEViLbzwcMkoC6S6mHJX0tkPitqxb0838MdPrZO\nEHfqHpu9Y5VYWIC5FaWPzMtiwwKBgQDUCJx6QWpHe7j2pwANJcAh4BZQheROmdnG\nr0pVziKOfEESm0bYD1xB2ZQtxNWaJYmwUWTaxlo1/dJHW8O2zX44PrwPRNZ4r5gY\nkZ7cnxf9pWCuA60qK9ooLsNCKD1789FUlOAwJ4CWWYloIEWygcJprj62WeACyaKw\nwum53f38VQKBgADNxs1/qiyLo/MhOCor+7loSEoPfzXrlpA0SO+J26QdzhnbNkFx\nvr+xaMvlewWwwhD4TelmpfWQBhArMOa8PWNAT4jkaRKDEfQw6nkBYbpLxUEpQFP5\nJoqM7xsXJlTiuQIRI1wsZcI6jhEfEMWaUIy8pZExMGMniOcWJ4QgD965AoGBAMVh\nfskQPC9vLS/vJk0W51ShliQ/f9jrv58Fbt2RlvmtEaaQhdJ7+hYSxa4VngJxD0vj\neU9vdmrsbeOfuQFjKiyRud885apTS/MTHB+kumCUovta0MiBKgReA8aCTzokLqne\nLRSmsT1E/HTCFh+mS6S1YAvAfpgZvClwSMONs/JBAoGAbx008z57aH6iXNwQsU0+\nKApNLreyl9tD39HBRng44TSF2ldArBTJFPyqrGsfKAfiZGb3bMso2jBx949Tyi9I\nqc1Z67XSU07lyXBngd6/qNbUgibvvd5wtN0fSHJPJNxlgPE+bqkZHEHyKONdPkpD\ny0v4ZXZL5w1mxDR/YNcxPQU=\n-----END PRIVATE KEY-----\n",
  "client_email": "stadtjefoehl-streamlit-sheets@stadtjefoehl.iam.gserviceaccount.com",
  "client_id": "106939897770585768125",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/stadtjefoehl-streamlit-sheets%40stadtjefoehl.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
- GSHEET_ID = "1RGkpRAPTX9Zm95BNLAwBP74Cvc8sbg9ITUU2bhp7hQI"
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
    info = json.loads(st.secrets["GSPREAD_SERVICE_ACCOUNT"])
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
    col_duration: str = "Dauer"  # NEU

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

    out["date"]    = pd.to_datetime(df[cmap.col_date], errors="coerce").dt.date
    out["time"]    = df[cmap.col_time].astype(str)
    out["event"]   = df[cmap.col_event]
    out["address"] = df[cmap.col_address]
    out["venue"]   = df[cmap.col_venue]
    out["city"]    = df[cmap.col_city]
    out["duration"]= df[cmap.col_duration]

    # stabile event_id
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
# Google Sheets: Schreiben (Julian / Valentin)
# -------------------------------------------------
def write_status_to_excel(sheet_row_index_1based: int, tech_name: str, status: str) -> None:
    """Schreibt den Status in die Spalte des Technikers (Julian/Valentin) in derselben Zeile."""
    gc = get_gspread_client()
    sh = gc.open_by_key(st.secrets["GSHEET_ID"])
    ws = sh.worksheet(SHEET_NAME)

    headers = ws.row_values(1)  # Header-Zeile
    try:
        col_index_1based = [h.strip().lower() for h in headers].index(tech_name.strip().lower()) + 1
    except ValueError:
        raise ValueError(f"Spalte '{tech_name}' nicht gefunden. Bitte im Sheet anlegen.")

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

    # Sidebar: Logo + Name
    with st.sidebar:
        st.image("logo.png", width=160)
        st.header("👤 Dein Name")
        tech_name = st.text_input("Name für Eintragung", value="", placeholder="Dein Name")

        # Optional: schneller Verbindungs-Test zu Google Sheets
        # if st.button("🔌 Sheets-Test"):
        #     try:
        #         gc = get_gspread_client()
        #         sh = gc.open_by_key(st.secrets["GSHEET_ID"])
        #         st.success(f"Verbunden: {sh.title}")
        #     except Exception as e:
        #         st.error(f"Sheets-Verbindung fehlgeschlagen: {e}")

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
            # Adresse & Dauer nur im Expander
            if isinstance(row.get("address"), str) and row["address"].strip():
                st.markdown(f"**📍 Adresse:** {row['address']}")
            dur_text = str(row.get("duration") or "").strip()
            if dur_text:
                st.markdown(f"**⏱ Dauer:** {dur_text}")

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
                    # Für Julian/Valentin zusätzlich ins Google Sheet schreiben
                    if tech_name.strip().lower() in {"julian", "valentin"}:
                        try:
                            write_status_to_excel(int(row["xl_row"]), tech_name.strip(), status)
                            st.success("Gespeichert (App + Google Sheet).")
                        except Exception as ex:
                            st.warning(f"Gespeichert in App. Google-Sheet-Update fehlgeschlagen: {ex}")
                    else:
                        st.success("Gespeichert.")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.warning("Bitte gib links in der Seitenleiste deinen Namen ein.")

if __name__ == "__main__":
    main()
