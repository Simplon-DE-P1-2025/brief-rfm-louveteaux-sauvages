"""
Tableau de bord RFM : KPIs, repartition par segment et visualisations.
Sources : public.rfm_scores (prioritaire), sinon public.v_rfm_segmentation.
"""

import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


@st.cache_data(ttl=120)
def load_rfm_from_scores() -> pd.DataFrame | None:
    conn = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        df = pd.read_sql_query(
            """
            SELECT
                customer_id,
                recency,
                frequency,
                monetary,
                r_score,
                f_score,
                m_score,
                rfm_score,
                rfm_total,
                segment,
                categorisation
            FROM public.rfm_scores
            """,
            conn,
        )
    except Exception:
        return None
    finally:
        conn.close()
    return df if df is not None and not df.empty else None


@st.cache_data(ttl=120)
def load_rfm_from_view() -> pd.DataFrame | None:
    conn = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        df = pd.read_sql_query(
            """
            SELECT
                customer_id,
                recency,
                frequency,
                monetary,
                r_score,
                f_score,
                m_score,
                rfm_score,
                rfm_total,
                segment
            FROM public.v_rfm_segmentation
            """,
            conn,
        )
    except Exception:
        return None
    finally:
        conn.close()
    return df if df is not None and not df.empty else None


def _clear_cache():
    load_rfm_from_scores.clear()
    load_rfm_from_view.clear()


def _fmt_int(n: float) -> str:
    return f"{int(round(n)):,}".replace(",", " ")


def _fmt_eur(n: float) -> str:
    return f"{n:,.0f} €".replace(",", " ")


st.set_page_config(page_title="RFM — Segmentation", layout="wide")

header_l, header_r = st.columns([4, 1], vertical_alignment="top")
with header_l:
    st.title("Segmentation RFM")
    st.caption(
        "Indicateurs et graphiques a partir du pipeline Airflow "
        "(tables `rfm_scores` / vue `v_rfm_segmentation`)."
    )
with header_r:
    st.markdown("<div style='height: 0.75rem;'></div>", unsafe_allow_html=True)
    if st.button("Actualiser", use_container_width=True):
        _clear_cache()
        st.rerun()

try:
    df = load_rfm_from_scores()
    source_label = "public.rfm_scores"
    if df is None:
        df = load_rfm_from_view()
        source_label = "public.v_rfm_segmentation"
except Exception as err:
    st.error(f"Connexion ou lecture base impossible : {err}")
    st.stop()

if df is None or df.empty:
    st.warning(
        "Aucune donnee RFM trouvee. Executer le DAG `rfm_pipeline` (taches ingest → load) "
        "puis recharger cette page."
    )
    st.stop()

# Types numeriques
for col in ("recency", "frequency", "monetary", "r_score", "f_score", "m_score", "rfm_total"):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

st.success(f"Source : **{source_label}** — {_fmt_int(len(df))} clients")

# --- KPIs globaux ---
total_ca = float(df["monetary"].sum())
avg_r = float(df["recency"].mean())
avg_f = float(df["frequency"].mean())
avg_m = float(df["monetary"].mean())
median_m = float(df["monetary"].median())
nb_segments = int(df["segment"].nunique())
champions = int((df["segment"] == "Champion").sum()) if "segment" in df.columns else 0
pct_champions = 100.0 * champions / len(df) if len(df) else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Clients actifs (RFM)", _fmt_int(len(df)))
k2.metric("CA total (monetary)", _fmt_eur(total_ca))
k3.metric("Panier moyen / client", _fmt_eur(avg_m))
k4.metric("Recency moyenne (j)", f"{avg_r:.0f}")
k5.metric("Frequency moyenne", f"{avg_f:.2f}")

k6, k7, k8, k9, k10 = st.columns(5)
k6.metric("Monetary median", _fmt_eur(median_m))
k7.metric("Segments distincts", str(nb_segments))
k8.metric("Champions", _fmt_int(champions))
k9.metric("% Champions", f"{pct_champions:.1f} %")
if "categorisation" in df.columns:
    vip = int((df["categorisation"] == "VIP").sum())
    k10.metric("Clients VIP", _fmt_int(vip))
else:
    k10.metric("Score RFM moyen", f"{df['rfm_total'].mean():.1f}")

st.divider()

# --- Agregations segment ---
seg_stats = (
    df.groupby("segment", dropna=False)
    .agg(
        nb_clients=("customer_id", "count"),
        ca=("monetary", "sum"),
        recency_moy=("recency", "mean"),
        freq_moy=("frequency", "mean"),
        monetary_moy=("monetary", "mean"),
    )
    .reset_index()
    .sort_values("ca", ascending=False)
)
seg_stats["pct_clients"] = 100.0 * seg_stats["nb_clients"] / len(df)
seg_stats["pct_ca"] = 100.0 * seg_stats["ca"] / total_ca if total_ca else 0.0

left, right = st.columns(2)

with left:
    st.subheader("Clients par segment")
    c1 = (
        alt.Chart(seg_stats)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X("nb_clients:Q", title="Nombre de clients"),
            y=alt.Y("segment:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("segment:N", title="Segment"),
                alt.Tooltip("nb_clients:Q", title="Clients"),
                alt.Tooltip("pct_clients:Q", format=".1f", title="% clients"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(c1, use_container_width=True)

with right:
    st.subheader("CA par segment")
    c2 = (
        alt.Chart(seg_stats)
        .mark_bar(color="#2E7D32", cornerRadiusEnd=4)
        .encode(
            x=alt.X("ca:Q", title="CA (monetary)"),
            y=alt.Y("segment:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("segment:N", title="Segment"),
                alt.Tooltip("ca:Q", format=",.0f", title="CA"),
                alt.Tooltip("pct_ca:Q", format=".1f", title="% CA"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(c2, use_container_width=True)

# --- Categorisation (si presente) ---
if "categorisation" in df.columns and df["categorisation"].notna().any():
    st.subheader("Categorisation (score total)")
    cat_stats = (
        df.groupby("categorisation", dropna=False)
        .agg(nb_clients=("customer_id", "count"), ca=("monetary", "sum"))
        .reset_index()
        .sort_values("nb_clients", ascending=False)
    )
    c3, c4 = st.columns(2)
    with c3:
        pie = (
            alt.Chart(cat_stats)
            .mark_arc(innerRadius=50)
            .encode(
                theta=alt.Theta("nb_clients:Q", stack=True),
                color=alt.Color("categorisation:N", legend=alt.Legend(title=None)),
                tooltip=[
                    alt.Tooltip("categorisation:N", title="Categorie"),
                    alt.Tooltip("nb_clients:Q", title="Clients"),
                    alt.Tooltip("ca:Q", format=",.0f", title="CA"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(pie, use_container_width=True)
    with c4:
        st.dataframe(
            cat_stats.assign(
                pct_clients=lambda x: 100.0 * x["nb_clients"] / len(df),
                pct_ca=lambda x: 100.0 * x["ca"] / total_ca if total_ca else 0.0,
            ).style.format(
                {"ca": "{:,.0f}", "pct_clients": "{:.1f} %", "pct_ca": "{:.1f} %"}
            ),
            use_container_width=True,
            hide_index=True,
        )

st.divider()
st.subheader("Matrices et distributions")

h1, h2 = st.columns(2)

with h1:
    st.markdown("**Heatmap R x F** (nombre de clients)")
    heat = (
        df.groupby(["r_score", "f_score"], dropna=False)
        .size()
        .reset_index(name="nb")
    )
    hm = (
        alt.Chart(heat)
        .mark_rect()
        .encode(
            x=alt.X("r_score:O", title="R score"),
            y=alt.Y("f_score:O", title="F score"),
            color=alt.Color("nb:Q", scale=alt.Scale(scheme="blues"), legend=alt.Legend(title="Clients")),
            tooltip=["r_score", "f_score", "nb"],
        )
        .properties(height=260)
    )
    st.altair_chart(hm, use_container_width=True)

with h2:
    st.markdown("**Heatmap R x F** (CA moyen par case)")
    heat_m = (
        df.groupby(["r_score", "f_score"], dropna=False)["monetary"]
        .mean()
        .reset_index(name="ca_moy")
    )
    hm2 = (
        alt.Chart(heat_m)
        .mark_rect()
        .encode(
            x=alt.X("r_score:O", title="R score"),
            y=alt.Y("f_score:O", title="F score"),
            color=alt.Color(
                "ca_moy:Q",
                scale=alt.Scale(scheme="greens"),
                legend=alt.Legend(title="CA moy."),
            ),
            tooltip=[
                alt.Tooltip("r_score:O"),
                alt.Tooltip("f_score:O"),
                alt.Tooltip("ca_moy:Q", format=",.0f", title="CA moyen"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(hm2, use_container_width=True)

d1, d2, d3 = st.columns(3)

with d1:
    st.markdown("**Distribution recency (jours)**")
    hr = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            alt.X("recency:Q", bin=alt.Bin(maxbins=40), title="Recency"),
            alt.Y("count()", title="Effectif"),
        )
        .properties(height=220)
    )
    st.altair_chart(hr, use_container_width=True)

with d2:
    st.markdown("**Distribution frequency**")
    hf = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            alt.X("frequency:Q", bin=alt.Bin(maxbins=30), title="Frequency"),
            alt.Y("count()", title="Effectif"),
        )
        .properties(height=220)
    )
    st.altair_chart(hf, use_container_width=True)

with d3:
    st.markdown("**Distribution monetary**")
    hm_hist = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            alt.X("monetary:Q", bin=alt.Bin(maxbins=35), title="Monetary"),
            alt.Y("count()", title="Effectif"),
        )
        .properties(height=220)
    )
    st.altair_chart(hm_hist, use_container_width=True)

st.markdown("**Recency vs monetary** (echantillon max 5000 points)")
sample = df.sample(min(5000, len(df)), random_state=42) if len(df) > 5000 else df
scatter = (
    alt.Chart(sample)
    .mark_circle(size=30, opacity=0.35)
    .encode(
        x=alt.X("recency:Q", title="Recency (jours)"),
        y=alt.Y("monetary:Q", title="Monetary"),
        color=alt.Color("segment:N", legend=alt.Legend(title="Segment")),
        tooltip=["customer_id", "segment", "recency", "frequency", "monetary", "rfm_score"],
    )
    .properties(height=380)
)
st.altair_chart(scatter, use_container_width=True)

st.divider()
st.subheader("Top clients par monetary")
top_n = st.slider("Nombre de lignes", min_value=10, max_value=100, value=25, step=5)
top_df = df.nlargest(top_n, "monetary")[
    [
        c
        for c in [
            "customer_id",
            "segment",
            "categorisation",
            "recency",
            "frequency",
            "monetary",
            "rfm_score",
            "rfm_total",
        ]
        if c in df.columns
    ]
]
st.dataframe(top_df, use_container_width=True, hide_index=True)

with st.expander("Table agregee par segment (export / detail)"):
    st.dataframe(
        seg_stats.style.format(
            {
                "ca": "{:,.0f}",
                "recency_moy": "{:.1f}",
                "freq_moy": "{:.2f}",
                "monetary_moy": "{:,.0f}",
                "pct_clients": "{:.1f} %",
                "pct_ca": "{:.1f} %",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
