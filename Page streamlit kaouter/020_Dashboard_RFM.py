"""
Dashboard RFM — Vue équipes métiers
Toutes les visualisations sont construites depuis clean.fact_orders.
"""
import os
from pathlib import Path

import altair as alt
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from dotenv import load_dotenv

# Charge le .env depuis la racine du projet (2 niveaux au-dessus de pages/)
load_dotenv(Path(__file__).parents[2] / ".env")

# ─────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard RFM",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────
# PALETTE SEGMENTS
# ─────────────────────────────────────────────────────────
SEGMENT_COLORS = {
    "Champion":        "#2ecc71",
    "Client fidele":   "#3498db",
    "Nouveau client":  "#9b59b6",
    "Client moyen":    "#f39c12",
    "Client a risque": "#e67e22",
    "Client perdu":    "#e74c3c",
}
CATEG_COLORS = {
    "VIP":      "#2ecc71",
    "Fidele":   "#3498db",
    "Regulier": "#9b59b6",
    "A risque": "#e67e22",
    "Inactif":  "#e74c3c",
}

# ─────────────────────────────────────────────────────────
# CONNEXION DB
# ─────────────────────────────────────────────────────────
def _cfg(key: str, default: str = "") -> str:
    try:
        return str(st.secrets[key])
    except Exception:
        return os.getenv(key, default)


@st.cache_resource
def _get_engine():
    u = _cfg("POSTGRES_USER", "postgres")
    p = _cfg("POSTGRES_PASSWORD", "postgres")
    h = _cfg("POSTGRES_HOST", "localhost")
    port = _cfg("POSTGRES_PORT", "5432")
    db = _cfg("POSTGRES_DB", "rfm_db")
    return create_engine(f"postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}")


# ─────────────────────────────────────────────────────────
# CHARGEMENT DONNÉES
# ─────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Chargement des données…")
def load_fact_orders() -> pd.DataFrame:
    engine = _get_engine()
    df = pd.read_sql(
        """
        SELECT
            f.invoice,
            f.stock_code,
            f.customer_id,
            f.quantity,
            f.price,
            f.total_line,
            f.r_score, f.f_score, f.m_score,
            f.rfm_score, f.rfm_total,
            f.segment,
            f.categorisation,
            -- depuis dim_facture
            fa.invoice_date,
            -- depuis dim_produit
            p.description,
            -- depuis dim_client
            c.country
        FROM  clean.fact_orders  f
        JOIN  clean.dim_facture  fa ON fa.invoice     = f.invoice
        JOIN  clean.dim_produit  p  ON p.stock_code   = f.stock_code
        JOIN  clean.dim_client   c  ON c.customer_id  = f.customer_id
        """,
        engine,
    )
    df["invoice_date"] = pd.to_datetime(df["invoice_date"])
    df["r_score"]      = df["r_score"].astype(int)
    df["f_score"]      = df["f_score"].astype(int)
    df["m_score"]      = df["m_score"].astype(int)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_rfm_clients() -> pd.DataFrame:
    engine = _get_engine()
    return pd.read_sql(
        """
        SELECT DISTINCT
            customer_id, country,
            r_score, f_score, m_score,
            rfm_score, rfm_total,
            segment, categorisation
        FROM clean.fact_orders
        """,
        engine,
    )


# ─────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────
def _delta(val: float, ref: float, fmt: str = ".0f") -> str:
    if ref == 0:
        return ""
    d = val - ref
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:{fmt}}"


def kpi_card(col, label: str, value, delta: str = "", icon: str = ""):
    col.metric(label=f"{icon} {label}", value=value, delta=delta or None)


# ─────────────────────────────────────────────────────────
# SIDEBAR — FILTRES
# ─────────────────────────────────────────────────────────
def build_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.image(
        "https://cdn-icons-png.flaticon.com/512/4727/4727266.png", width=60
    )
    st.sidebar.title("🎛️ Filtres")

    # Période
    st.sidebar.markdown("### 📅 Période")
    min_d = df["invoice_date"].min().date()
    max_d = df["invoice_date"].max().date()
    date_range = st.sidebar.date_input(
        "Plage de dates",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d,
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        d_start, d_end = date_range
    else:
        d_start, d_end = min_d, max_d

    # Pays
    st.sidebar.markdown("### 🌍 Pays")
    pays_liste = sorted(df["country"].dropna().unique())
    pays_sel = st.sidebar.multiselect(
        "Pays", options=pays_liste, default=[], placeholder="Tous les pays"
    )

    # Segments
    st.sidebar.markdown("### 🏷️ Segment client")
    seg_liste = sorted(df["segment"].dropna().unique())
    seg_sel = st.sidebar.multiselect(
        "Segments", options=seg_liste, default=[], placeholder="Tous les segments"
    )

    # Catégorisation
    st.sidebar.markdown("### 🎖️ Catégorisation")
    cat_liste = sorted(df["categorisation"].dropna().unique())
    cat_sel = st.sidebar.multiselect(
        "Catégorisations", options=cat_liste, default=[], placeholder="Toutes"
    )

    # Score RFM total
    st.sidebar.markdown("### 🔢 Score RFM total")
    rfm_min, rfm_max = int(df["rfm_total"].min()), int(df["rfm_total"].max())
    rfm_range = st.sidebar.slider(
        "Fourchette rfm_total", min_value=rfm_min, max_value=rfm_max,
        value=(rfm_min, rfm_max)
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("Données : Online Retail II — UCI")

    # Application des filtres
    mask = (
        (df["invoice_date"].dt.date >= d_start)
        & (df["invoice_date"].dt.date <= d_end)
        & (df["rfm_total"] >= rfm_range[0])
        & (df["rfm_total"] <= rfm_range[1])
    )
    if pays_sel:
        mask &= df["country"].isin(pays_sel)
    if seg_sel:
        mask &= df["segment"].isin(seg_sel)
    if cat_sel:
        mask &= df["categorisation"].isin(cat_sel)

    return df[mask].copy()


# ─────────────────────────────────────────────────────────
# ONGLETS
# ─────────────────────────────────────────────────────────
def tab_kpis(df: pd.DataFrame, clients: pd.DataFrame):
    """KPIs globaux."""
    total_ca      = df["total_line"].sum()
    nb_clients    = df["customer_id"].nunique()
    nb_commandes  = df["invoice"].nunique()
    panier_moyen  = total_ca / nb_commandes if nb_commandes else 0

    champions     = clients[clients["segment"] == "Champion"]["customer_id"].nunique()
    a_risque      = clients[clients["segment"] == "Client a risque"]["customer_id"].nunique()
    perdus        = clients[clients["segment"] == "Client perdu"]["customer_id"].nunique()
    vip           = clients[clients["categorisation"] == "VIP"]["customer_id"].nunique()

    st.markdown("## 📈 Indicateurs clés de performance")
    c1, c2, c3, c4 = st.columns(4)
    kpi_card(c1, "Chiffre d'affaires",  f"{total_ca:,.0f} €",  icon="💶")
    kpi_card(c2, "Clients actifs",       f"{nb_clients:,}",     icon="👥")
    kpi_card(c3, "Commandes",            f"{nb_commandes:,}",   icon="🛒")
    kpi_card(c4, "Panier moyen",         f"{panier_moyen:,.2f} €", icon="🧺")

    st.markdown("---")
    c5, c6, c7, c8 = st.columns(4)
    kpi_card(c5, "Champions",      f"{champions:,}",  icon="🏆")
    kpi_card(c6, "Clients VIP",    f"{vip:,}",        icon="⭐")
    kpi_card(c7, "Clients à risque", f"{a_risque:,}", icon="⚠️")
    kpi_card(c8, "Clients perdus", f"{perdus:,}",     icon="❌")


def tab_segments(df: pd.DataFrame, clients: pd.DataFrame):
    """Répartition des segments et catégorisations."""
    st.markdown("## 🏷️ Répartition des clients")

    col1, col2 = st.columns(2)

    # ── Segments ──
    seg_counts = (
        clients.groupby("segment")["customer_id"]
        .nunique()
        .reset_index(name="nb_clients")
        .sort_values("nb_clients", ascending=False)
    )
    seg_counts["color"] = seg_counts["segment"].map(SEGMENT_COLORS)

    chart_seg = (
        alt.Chart(seg_counts)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("nb_clients:Q", title="Nombre de clients"),
            y=alt.Y("segment:N", sort="-x", title=""),
            color=alt.Color(
                "segment:N",
                scale=alt.Scale(
                    domain=list(SEGMENT_COLORS.keys()),
                    range=list(SEGMENT_COLORS.values()),
                ),
                legend=None,
            ),
            tooltip=["segment", "nb_clients"],
        )
        .properties(title="Segments RFM", height=300)
    )
    col1.altair_chart(chart_seg, use_container_width=True)

    # ── Catégorisations ──
    cat_counts = (
        clients.groupby("categorisation")["customer_id"]
        .nunique()
        .reset_index(name="nb_clients")
        .sort_values("nb_clients", ascending=False)
    )

    chart_cat = (
        alt.Chart(cat_counts)
        .mark_arc(innerRadius=60)
        .encode(
            theta=alt.Theta("nb_clients:Q"),
            color=alt.Color(
                "categorisation:N",
                scale=alt.Scale(
                    domain=list(CATEG_COLORS.keys()),
                    range=list(CATEG_COLORS.values()),
                ),
                legend=alt.Legend(title="Catégorie"),
            ),
            tooltip=["categorisation", "nb_clients"],
        )
        .properties(title="Catégorisations (donut)", height=300)
    )
    col2.altair_chart(chart_cat, use_container_width=True)

    # ── Tableau récapitulatif ──
    st.markdown("### 📋 Tableau récapitulatif par segment")
    summary = (
        df.groupby("segment")
        .agg(
            nb_clients   = ("customer_id", "nunique"),
            nb_commandes = ("invoice",     "nunique"),
            ca_total     = ("total_line",  "sum"),
        )
        .reset_index()
        .sort_values("ca_total", ascending=False)
    )
    summary["panier_moyen"]    = (summary["ca_total"] / summary["nb_commandes"]).round(2)
    summary["ca_total"]        = summary["ca_total"].round(2)
    summary["% clients"]       = (summary["nb_clients"] / summary["nb_clients"].sum() * 100).round(1)

    st.dataframe(
        summary.rename(columns={
            "segment":      "Segment",
            "nb_clients":   "Clients",
            "nb_commandes": "Commandes",
            "ca_total":     "CA (€)",
            "panier_moyen": "Panier moyen (€)",
            "% clients":    "% Clients",
        }),
        use_container_width=True,
        hide_index=True,
    )


def tab_rfm_analyse(clients: pd.DataFrame):
    """Scatter RFM, heatmap et distributions."""
    st.markdown("## 🔬 Analyse RFM")

    col1, col2 = st.columns([3, 2])

    # ── Scatter Recency vs Monetary ──
    scatter = (
        alt.Chart(clients.sample(min(3000, len(clients)), random_state=42))
        .mark_circle(opacity=0.6, size=40)
        .encode(
            x=alt.X("r_score:O", title="Score Recency (5=récent)"),
            y=alt.Y("m_score:O", title="Score Monetary (5=dépensier)"),
            color=alt.Color(
                "segment:N",
                scale=alt.Scale(
                    domain=list(SEGMENT_COLORS.keys()),
                    range=list(SEGMENT_COLORS.values()),
                ),
                legend=alt.Legend(title="Segment"),
            ),
            size=alt.Size("f_score:Q", legend=alt.Legend(title="Fréquence")),
            tooltip=["customer_id", "segment", "r_score", "f_score", "m_score", "rfm_total"],
        )
        .properties(title="Recency × Monetary (taille = Fréquence)", height=350)
        .interactive()
    )
    col1.altair_chart(scatter, use_container_width=True)

    # ── Distribution rfm_total ──
    hist = (
        alt.Chart(clients)
        .mark_bar(color="#3498db", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("rfm_total:O", title="Score RFM total"),
            y=alt.Y("count():Q", title="Nombre de clients"),
            tooltip=["rfm_total", "count()"],
        )
        .properties(title="Distribution du score RFM total", height=350)
    )
    col2.altair_chart(hist, use_container_width=True)

    # ── Heatmap R × F ──
    st.markdown("### 🗺️ Heatmap Recency × Frequency")
    heatmap_data = (
        clients.groupby(["r_score", "f_score"])["customer_id"]
        .nunique()
        .reset_index(name="nb_clients")
    )
    heatmap = (
        alt.Chart(heatmap_data)
        .mark_rect()
        .encode(
            x=alt.X("f_score:O", title="Score Fréquence →"),
            y=alt.Y("r_score:O", sort="descending", title="← Score Recency"),
            color=alt.Color(
                "nb_clients:Q",
                scale=alt.Scale(scheme="blues"),
                legend=alt.Legend(title="Clients"),
            ),
            tooltip=["r_score", "f_score", "nb_clients"],
        )
        .properties(height=320)
    )
    st.altair_chart(heatmap, use_container_width=True)


def tab_ventes(df: pd.DataFrame):
    """Évolution CA, top produits, top pays."""
    st.markdown("## 💰 Analyse des ventes")

    # ── CA mensuel ──
    df_mensuel = (
        df.assign(mois=df["invoice_date"].dt.to_period("M").astype(str))
        .groupby("mois")["total_line"]
        .sum()
        .reset_index(name="ca")
        .sort_values("mois")
    )
    line = (
        alt.Chart(df_mensuel)
        .mark_area(line=True, color="#3498db", opacity=0.3)
        .encode(
            x=alt.X("mois:O", title="Mois", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("ca:Q", title="CA (€)"),
            tooltip=["mois", "ca"],
        )
        .properties(title="Évolution mensuelle du chiffre d'affaires", height=300)
    )
    st.altair_chart(line, use_container_width=True)

    col1, col2 = st.columns(2)

    # ── Top 10 produits ──
    top_produits = (
        df.groupby(["stock_code", "description"])["total_line"]
        .sum()
        .reset_index(name="ca")
        .sort_values("ca", ascending=False)
        .head(10)
    )
    top_produits["label"] = top_produits["description"].str[:30]
    chart_prod = (
        alt.Chart(top_produits)
        .mark_bar(color="#2ecc71", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("ca:Q", title="CA (€)"),
            y=alt.Y("label:N", sort="-x", title=""),
            tooltip=["description", "ca"],
        )
        .properties(title="Top 10 produits par CA", height=320)
    )
    col1.altair_chart(chart_prod, use_container_width=True)

    # ── Top 10 pays ──
    top_pays = (
        df.groupby("country")["total_line"]
        .sum()
        .reset_index(name="ca")
        .sort_values("ca", ascending=False)
        .head(10)
    )
    chart_pays = (
        alt.Chart(top_pays)
        .mark_bar(color="#9b59b6", cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("ca:Q", title="CA (€)"),
            y=alt.Y("country:N", sort="-x", title=""),
            tooltip=["country", "ca"],
        )
        .properties(title="Top 10 pays par CA", height=320)
    )
    col2.altair_chart(chart_pays, use_container_width=True)


def tab_clients(df: pd.DataFrame, clients: pd.DataFrame):
    """Fiche client + tableau filtrable."""
    st.markdown("## 👤 Exploration clients")

    col_search, col_seg, col_cat = st.columns([2, 2, 2])

    # Recherche par ID
    q_id = col_search.text_input("🔍 Rechercher un client (customer_id)", "")

    # Filtres inline
    seg_opts = ["Tous"] + sorted(clients["segment"].unique().tolist())
    sel_seg  = col_seg.selectbox("Segment", seg_opts)

    cat_opts = ["Toutes"] + sorted(clients["categorisation"].unique().tolist())
    sel_cat  = col_cat.selectbox("Catégorisation", cat_opts)

    # Tri
    col_sort, col_ord = st.columns([2, 2])
    sort_col = col_sort.selectbox(
        "Trier par", ["rfm_total", "r_score", "f_score", "m_score"], index=0
    )
    sort_asc = col_ord.radio("Ordre", ["Décroissant", "Croissant"], horizontal=True) == "Croissant"

    # Application des filtres
    view = clients.copy()
    if q_id:
        view = view[view["customer_id"].astype(str).str.contains(q_id, case=False)]
    if sel_seg != "Tous":
        view = view[view["segment"] == sel_seg]
    if sel_cat != "Toutes":
        view = view[view["categorisation"] == sel_cat]
    view = view.sort_values(sort_col, ascending=sort_asc)

    st.markdown(f"**{len(view):,} clients** correspondants")
    st.dataframe(
        view.rename(columns={
            "customer_id":    "Client ID",
            "country":        "Pays",
            "r_score":        "R",
            "f_score":        "F",
            "m_score":        "M",
            "rfm_score":      "Score RFM",
            "rfm_total":      "Total",
            "segment":        "Segment",
            "categorisation": "Catégorie",
        }),
        use_container_width=True,
        hide_index=True,
        height=420,
    )

    # ── Fiche détail client ──
    st.markdown("---")
    st.markdown("### 📄 Fiche client")
    client_ids = sorted(view["customer_id"].astype(str).unique())
    if client_ids:
        selected_id = st.selectbox("Sélectionner un client", client_ids)
        row = clients[clients["customer_id"].astype(str) == selected_id].iloc[0]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Score R", row["r_score"])
        c2.metric("Score F", row["f_score"])
        c3.metric("Score M", row["m_score"])
        c4.metric("Segment", row["segment"])
        c5.metric("Catégorie", row["categorisation"])

        # Historique commandes de ce client
        histos = (
            df[df["customer_id"].astype(str) == selected_id]
            .groupby(df["invoice_date"].dt.to_period("M").astype(str))["total_line"]
            .sum()
            .reset_index()
        )
        histos.columns = ["mois", "ca"]
        if not histos.empty:
            mini_chart = (
                alt.Chart(histos)
                .mark_bar(color="#3498db")
                .encode(
                    x=alt.X("mois:O", title="Mois", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("ca:Q", title="CA (€)"),
                    tooltip=["mois", "ca"],
                )
                .properties(title=f"CA mensuel — client {selected_id}", height=220)
            )
            st.altair_chart(mini_chart, use_container_width=True)


def tab_export(df: pd.DataFrame, clients: pd.DataFrame):
    """Export CSV des données filtrées."""
    st.markdown("## 📥 Export des données")

    st.info(
        "Les exports respectent les filtres sélectionnés dans la barre latérale.",
        icon="ℹ️",
    )
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### Clients RFM")
        st.dataframe(clients.head(5), use_container_width=True, hide_index=True)
        st.download_button(
            "⬇️ Télécharger clients_rfm.csv",
            data=clients.to_csv(index=False).encode("utf-8"),
            file_name="clients_rfm.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with c2:
        st.markdown("### Lignes de commandes")
        st.dataframe(df.head(5), use_container_width=True, hide_index=True)
        st.download_button(
            "⬇️ Télécharger fact_orders.csv",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="fact_orders.csv",
            mime="text/csv",
            use_container_width=True,
        )


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────s
def main():
    st.title("📊 Dashboard RFMs")
    st.caption(
        "Online Retail II · Analyse Rsecency · Frequency · Monetary"
    )

    try:
        df_raw = load_fact_orders()
    except Exception as e:
        st.error(f"❌ Impossible de charger les données : {e}")
        st.info("Vérifiez que le pipeline ETL a bien été exécuté et que la table `clean.fact_orders` existe.")
        st.stop()

    # Filtres sidebar → df filtré
    df = build_sidebar(df_raw)

    if df.empty:
        st.warning("⚠️ Aucune donnée pour les filtres sélectionnés. Élargissez la sélection.")
        st.stop()

    # Clients uniques sur le df filtré
    clients_cols = [
        "customer_id", "country",
        "r_score", "f_score", "m_score",
        "rfm_score", "rfm_total",
        "segment", "categorisation",
    ]
    clients = df[clients_cols].drop_duplicates("customer_id")

    # Bandeau résumé filtre
    st.markdown(
        f"**Données affichées :** {df['invoice_date'].min().date()} → "
        f"{df['invoice_date'].max().date()} · "
        f"{clients['customer_id'].nunique():,} clients · "
        f"{df['invoice'].nunique():,} commandes"
    )

    # Onglets
    t1, t2, t3, t4, t5, t6 = st.tabs([
        "📈 KPIs",
        "🏷️ Segments",
        "🔬 Analyse RFM",
        "💰 Ventes",
        "👤 Clients",
        "📥 Export",
    ])
    with t1: tab_kpis(df, clients)
    with t2: tab_segments(df, clients)
    with t3: tab_rfm_analyse(clients)
    with t4: tab_ventes(df)
    with t5: tab_clients(df, clients)
    with t6: tab_export(df, clients)


if __name__ == "__main__":
    main()
