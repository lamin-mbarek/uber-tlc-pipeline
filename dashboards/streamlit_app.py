"""Dashboard interactif NYC TLC (fhvhv) — comparaison Uber vs Lyft.

Application Streamlit qui lit la table de faits ``trajets_nettoyes`` depuis
BigQuery (dataset ``tlc_analytics``), applique des filtres interactifs et calcule
les indicateurs et graphiques à la volée avec pandas.

Lancement :
    streamlit run dashboards/streamlit_app.py

Prérequis : identifiants ADC accessibles (``gcloud auth application-default
login``) et variables ``GCP_PROJECT`` / ``BQ_DATASET`` dans le fichier ``.env``.
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# --- Constantes ---------------------------------------------------------------
GCP_PROJECT = os.getenv("GCP_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "tlc_analytics")
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1")
TABLE_FAITS = f"{BQ_DATASET}.trajets_nettoyes"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Couleurs de plateforme : palette catégorielle validée (CVD-safe), une teinte
# par entité, jamais recyclée. Uber=bleu, Lyft=orange, etc.
COULEURS_PLATEFORME = {
    "Uber": "#2a78d6",
    "Lyft": "#eb6834",
    "Via": "#1baf7a",
    "Juno": "#eda100",
    "Autre": "#898781",
}

# Ordre des jours (jour_semaine : 1 = dimanche … 7 = samedi).
ORDRE_JOURS = {1: "Dimanche", 2: "Lundi", 3: "Mardi", 4: "Mercredi",
               5: "Jeudi", 6: "Vendredi", 7: "Samedi"}

# Encre et surfaces (design en thème clair assumé, verrouillé dans config.toml).
INK = "#1f2430"
MUTED = "#6b7280"
GRID = "#eceef2"
PLOTLY_FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif"

st.set_page_config(
    page_title="NYC TLC — Uber vs Lyft",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Style --------------------------------------------------------------------
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.4rem; padding-bottom: 2.5rem; max-width: 1400px;}

      /* Bandeau d'en-tete */
      .hero {
        background: linear-gradient(120deg, #0f2a4d 0%, #1f4e8c 55%, #2a78d6 100%);
        border-radius: 18px; padding: 22px 28px; color: #fff;
        box-shadow: 0 8px 26px rgba(15,42,77,0.28); margin-bottom: 18px;
      }
      .hero-title {font-size: 1.7rem; font-weight: 800; letter-spacing:-.01em; line-height:1.15;}
      .hero-sub {font-size: .95rem; opacity: .9; margin-top: 4px;}
      .hero-chips {margin-top: 14px; display:flex; gap:8px; flex-wrap:wrap;}
      .chip {display:inline-flex; align-items:center; gap:7px; font-size:.8rem;
             font-weight:600; padding:5px 12px; border-radius:999px;
             background: rgba(255,255,255,.14); backdrop-filter: blur(3px);}
      .dot {width:9px; height:9px; border-radius:50%; display:inline-block;}

      /* Cartes KPI */
      .kpi-card {
        position: relative; background:#ffffff; border:1px solid #edf0f4;
        border-radius:14px; padding:15px 16px 14px 18px; height:100%;
        box-shadow: 0 1px 2px rgba(16,24,40,.04);
        border-left: 4px solid var(--accent, #2a78d6);
        transition: transform .12s ease, box-shadow .12s ease;
      }
      .kpi-card:hover {transform: translateY(-2px); box-shadow:0 6px 18px rgba(16,24,40,.10);}
      .kpi-top {display:flex; justify-content:space-between; align-items:center;}
      .kpi-label {font-size:.72rem; color:#8a90a0; font-weight:600;
                  text-transform:uppercase; letter-spacing:.04em;}
      .kpi-icon {font-size:1.05rem; opacity:.85;}
      .kpi-value {font-size:1.62rem; font-weight:800; color:#1f2430; line-height:1.1;
                  margin-top:6px; font-variant-numeric: tabular-nums;}
      .kpi-sub {font-size:.74rem; color:#8a90a0; margin-top:2px;}

      /* Cartes de synthese plateforme */
      .plat-card {border-radius:14px; padding:16px 18px; color:#fff;
                  box-shadow:0 6px 18px rgba(16,24,40,.14);}
      .plat-name {font-size:1.15rem; font-weight:800;}
      .plat-metrics {display:flex; gap:20px; margin-top:8px;}
      .plat-metric small {display:block; font-size:.72rem; opacity:.85; font-weight:600;
                          text-transform:uppercase; letter-spacing:.03em;}
      .plat-metric b {font-size:1.25rem; font-weight:800; font-variant-numeric: tabular-nums;}

      .section-title {font-size:1.05rem; font-weight:700; color:#1f2430;
                      margin:2px 0 2px 0;}
      hr {margin: 1.1rem 0;}
    </style>
    """,
    unsafe_allow_html=True,
)


# --- Chargement des données ---------------------------------------------------
@st.cache_data(ttl=3600, show_spinner="Chargement des trajets depuis BigQuery…")
def charger_faits() -> pd.DataFrame:
    """Lit la table de faits depuis BigQuery et retourne un DataFrame pandas."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    df = client.query(
        f"SELECT * FROM `{TABLE_FAITS}`", location=BQ_LOCATION
    ).to_dataframe()
    df["date_trajet"] = pd.to_datetime(df["date_trajet"])
    df["nom_jour_fr"] = df["jour_semaine"].map(ORDRE_JOURS)
    return df


@st.cache_data(ttl=86400, show_spinner=False)
def charger_zones() -> pd.DataFrame | None:
    """Charge le référentiel des zones TLC (id -> arrondissement, nom)."""
    try:
        zones = pd.read_csv(ZONES_URL)
        return zones.rename(columns={
            "LocationID": "zone_id", "Borough": "arrondissement", "Zone": "zone_nom",
        })[["zone_id", "arrondissement", "zone_nom"]]
    except Exception:
        return None


# --- Utilitaires d'affichage --------------------------------------------------
def fmt_nombre(n) -> str:
    return f"{n:,.0f}".replace(",", " ")


def fmt_argent(n) -> str:
    if pd.isna(n):
        return "—"
    return f"{n:,.2f} $".replace(",", " ")


def carte_kpi(colonne, label, valeur, icon="", accent="#2a78d6", sous_titre="") -> None:
    """Affiche une carte KPI stylée (icône, accent coloré, sous-titre)."""
    colonne.markdown(
        f"""<div class="kpi-card" style="--accent:{accent}">
              <div class="kpi-top">
                <span class="kpi-label">{label}</span>
                <span class="kpi-icon">{icon}</span>
              </div>
              <div class="kpi-value">{valeur}</div>
              <div class="kpi-sub">{sous_titre}</div>
            </div>""",
        unsafe_allow_html=True,
    )


def carte_plateforme(colonne, nom, trajets, part, revenu_moyen) -> None:
    """Carte de synthèse d'une plateforme, aux couleurs de la marque."""
    couleur = COULEURS_PLATEFORME.get(nom, "#898781")
    colonne.markdown(
        f"""<div class="plat-card" style="background:linear-gradient(135deg,{couleur} 0%,{couleur}cc 100%)">
              <div class="plat-name">● {nom}</div>
              <div class="plat-metrics">
                <div class="plat-metric"><small>Trajets</small><b>{fmt_nombre(trajets)}</b></div>
                <div class="plat-metric"><small>Part</small><b>{part:.1f} %</b></div>
                <div class="plat-metric"><small>Revenu moyen</small><b>{revenu_moyen:.2f} $</b></div>
              </div>
            </div>""",
        unsafe_allow_html=True,
    )


def style_fig(fig: go.Figure, height=320, hovermode="x unified") -> go.Figure:
    """Applique la mise en forme commune aux graphiques Plotly."""
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=8, r=12, t=54, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
                    title_text=""),
        hovermode=hovermode,
        font=dict(family=PLOTLY_FONT, color=INK, size=13),
        title=dict(font=dict(size=15, color=INK), x=0, xanchor="left", y=0.97),
        uniformtext=dict(mode="hide", minsize=9),
    )
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor=GRID,
                     tickcolor=GRID, title_font=dict(color=MUTED, size=12),
                     tickfont=dict(color=MUTED))
    fig.update_yaxes(showgrid=True, gridcolor=GRID, zeroline=False,
                     linecolor="rgba(0,0,0,0)", title_font=dict(color=MUTED, size=12),
                     tickfont=dict(color=MUTED))
    return fig


# --- Chargement + garde d'erreur ----------------------------------------------
try:
    faits = charger_faits()
except Exception as erreur:  # auth ADC absente, dataset introuvable, etc.
    st.error(
        "Impossible de charger les données depuis BigQuery.\n\n"
        f"**{type(erreur).__name__}** : {erreur}\n\n"
        "Vérifiez `gcloud auth application-default login`, le fichier `.env` "
        "(GCP_PROJECT / BQ_DATASET) et que la table `trajets_nettoyes` existe."
    )
    st.stop()

zones = charger_zones()

# --- Sidebar : filtres --------------------------------------------------------
CLES_FILTRES = ["f_plateformes", "f_dates", "f_heures", "f_jour", "f_partage",
                "f_pmr", "f_arr"]


def reinitialiser_filtres():
    for cle in CLES_FILTRES:
        st.session_state.pop(cle, None)


st.sidebar.markdown("### 🔎 Filtres")

plateformes_dispo = sorted(faits["plateforme"].unique())
sel_plateformes = st.sidebar.multiselect(
    "Plateforme", plateformes_dispo, default=plateformes_dispo, key="f_plateformes"
)

d_min, d_max = faits["date_trajet"].min().date(), faits["date_trajet"].max().date()
sel_dates = st.sidebar.date_input(
    "Période", value=(d_min, d_max), min_value=d_min, max_value=d_max, key="f_dates"
)

sel_heures = st.sidebar.slider("Heure de prise en charge", 0, 23, (0, 23), key="f_heures")

sel_jour = st.sidebar.radio("Type de jour", ["Tous", "Semaine", "Week-end"],
                            horizontal=True, key="f_jour")

st.sidebar.markdown("**Options**")
col_o1, col_o2 = st.sidebar.columns(2)
sel_partage = col_o1.checkbox("Partagées", key="f_partage")
sel_pmr = col_o2.checkbox("PMR", key="f_pmr")

sel_arrondissements = None
if zones is not None:
    arr_dispo = sorted(zones["arrondissement"].dropna().unique())
    sel_arrondissements = st.sidebar.multiselect(
        "Arrondissement de départ", arr_dispo, key="f_arr"
    )

st.sidebar.button("↺ Réinitialiser les filtres", on_click=reinitialiser_filtres,
                  width="stretch")
st.sidebar.caption(
    f"Source : NYC TLC (fhvhv) échantillonnées · BigQuery `{BQ_DATASET}`."
)

# --- Application des filtres ---------------------------------------------------
df = faits[faits["plateforme"].isin(sel_plateformes)].copy()

if isinstance(sel_dates, (list, tuple)) and len(sel_dates) == 2:
    debut, fin = pd.Timestamp(sel_dates[0]), pd.Timestamp(sel_dates[1])
    df = df[(df["date_trajet"] >= debut) & (df["date_trajet"] <= fin)]

df = df[(df["heure"] >= sel_heures[0]) & (df["heure"] <= sel_heures[1])]

if sel_jour == "Semaine":
    df = df[~df["est_weekend"]]
elif sel_jour == "Week-end":
    df = df[df["est_weekend"]]

if sel_partage:
    df = df[df["est_partagee"]]
if sel_pmr:
    df = df[df["est_pmr"]]

if sel_arrondissements and zones is not None:
    ids = zones[zones["arrondissement"].isin(sel_arrondissements)]["zone_id"]
    df = df[df["PULocationID"].isin(ids)]

# --- En-tête (hero) -----------------------------------------------------------
chips = "".join(
    f'<span class="chip"><span class="dot" style="background:{COULEURS_PLATEFORME[p]}"></span>{p}</span>'
    for p in plateformes_dispo
)
st.markdown(
    f"""<div class="hero">
          <div class="hero-title">🚕 NYC TLC — Trajets VTC à haut volume</div>
          <div class="hero-sub">Analyse &amp; comparaison <b>Uber vs Lyft</b> · dashboard interactif</div>
          <div class="hero-chips">{chips}</div>
        </div>""",
    unsafe_allow_html=True,
)

if df.empty:
    st.warning("Aucun trajet ne correspond aux filtres sélectionnés.")
    st.stop()

# --- Bandeau KPI --------------------------------------------------------------
c = st.columns(5)
carte_kpi(c[0], "Trajets", fmt_nombre(len(df)), "🚗", "#2a78d6")
carte_kpi(c[1], "Revenu total", fmt_argent(df["revenu_total"].sum()), "💰", "#1baf7a")
carte_kpi(c[2], "Revenu moyen", fmt_argent(df["revenu_total"].mean()), "📈", "#2a78d6")
carte_kpi(c[3], "Pourboire moyen", fmt_argent(df["tips"].mean()), "🪙", "#eda100")
carte_kpi(c[4], "Taux reversement", f"{df['taux_reversement'].mean():.0%}", "🤝", "#7c6cf0")

st.write("")
c = st.columns(5)
carte_kpi(c[0], "Distance moyenne", f"{df['trip_miles'].mean():.2f} mi", "📏", "#2a78d6")
carte_kpi(c[1], "Durée moyenne", f"{df['duree_minutes'].mean():.1f} min", "⏱️", "#eb6834")
carte_kpi(c[2], "Attente moyenne", f"{df['temps_attente_min'].mean():.1f} min", "⏳", "#e34948")
carte_kpi(c[3], "Courses partagées", f"{100 * df['est_partagee'].mean():.1f} %", "👥", "#1baf7a")
carte_kpi(c[4], "Courses PMR", f"{100 * df['est_pmr'].mean():.2f} %", "♿", "#7c6cf0")

st.divider()

# --- Onglets ------------------------------------------------------------------
onglet_apercu, onglet_temps, onglet_zones, onglet_donnees = st.tabs(
    ["📊 Vue d'ensemble", "🕒 Analyse temporelle", "🗺️ Zones & revenu", "🔢 Données"]
)


def bar_par_plateforme(serie, titre, text_fmt=".2f"):
    """Barre verticale d'une mesure agrégée par plateforme."""
    d = serie.reset_index()
    d.columns = ["plateforme", "valeur"]
    fig = px.bar(
        d, x="plateforme", y="valeur", color="plateforme",
        color_discrete_map=COULEURS_PLATEFORME, title=titre, text_auto=text_fmt,
    )
    fig.update_traces(textposition="outside", textfont=dict(color=INK, size=12),
                      marker_line_width=0, cliponaxis=False,
                      hovertemplate="%{x} · %{y}<extra></extra>")
    fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)
    return style_fig(fig, height=300, hovermode="closest")


with onglet_apercu:
    g = df.groupby("plateforme")
    ordre = g.size().sort_values(ascending=False)
    total = len(df)

    st.markdown('<div class="section-title">Synthèse par plateforme</div>',
                unsafe_allow_html=True)
    cols_p = st.columns(max(len(ordre), 1))
    for col, (nom, n) in zip(cols_p, ordre.items()):
        carte_plateforme(col, nom, n, 100 * n / total, g["revenu_total"].mean()[nom])

    st.write("")
    st.markdown('<div class="section-title">Indicateurs comparés</div>',
                unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    col1.plotly_chart(bar_par_plateforme(g.size(), "Nombre de trajets", ",.0f"),
                      width="stretch")
    col2.plotly_chart(bar_par_plateforme(g["revenu_total"].mean().round(2),
                      "Revenu moyen ($)"), width="stretch")
    col3.plotly_chart(bar_par_plateforme(g["tips"].mean().round(2),
                      "Pourboire moyen ($)"), width="stretch")

    col4, col5, col6 = st.columns(3)
    col4.plotly_chart(bar_par_plateforme(g["vitesse_mph"].mean().round(2),
                      "Vitesse moyenne (mph)"), width="stretch")
    col5.plotly_chart(bar_par_plateforme(g["temps_attente_min"].mean().round(2),
                      "Attente moyenne (min)"), width="stretch")

    parts = g.size().reset_index(name="trajets")
    fig_part = px.pie(
        parts, names="plateforme", values="trajets", hole=0.62,
        color="plateforme", color_discrete_map=COULEURS_PLATEFORME,
        title="Part des trajets",
    )
    fig_part.update_traces(textposition="inside", textinfo="percent+label",
                           marker=dict(line=dict(color="#fff", width=2)))
    col6.plotly_chart(style_fig(fig_part, height=300, hovermode="closest"),
                      width="stretch")


with onglet_temps:
    st.markdown('<div class="section-title">Distribution temporelle</div>',
                unsafe_allow_html=True)

    par_heure = (
        df.groupby(["heure", "plateforme"]).size().reset_index(name="nombre_trajets")
    )
    fig_h = px.bar(
        par_heure, x="heure", y="nombre_trajets", color="plateforme",
        color_discrete_map=COULEURS_PLATEFORME, barmode="group",
        title="Trajets par heure de la journée",
    )
    fig_h.update_layout(xaxis=dict(dtick=1), xaxis_title="Heure", yaxis_title="Trajets",
                        bargap=0.15)
    st.plotly_chart(style_fig(fig_h, height=360), width="stretch")

    col_a, col_b = st.columns(2)

    par_jour = (
        df.groupby(["jour_semaine", "nom_jour_fr", "plateforme"])
        .size().reset_index(name="nombre_trajets").sort_values("jour_semaine")
    )
    fig_j = px.bar(
        par_jour, x="nom_jour_fr", y="nombre_trajets", color="plateforme",
        color_discrete_map=COULEURS_PLATEFORME, barmode="group",
        title="Trajets par jour de la semaine",
    )
    fig_j.update_layout(xaxis_title=None, yaxis_title="Trajets")
    col_a.plotly_chart(style_fig(fig_j, height=340), width="stretch")

    df["annee_mois"] = df["annee"].astype(str) + "-" + df["mois"].astype(str).str.zfill(2)
    par_mois = (
        df.groupby(["annee_mois", "plateforme"])["revenu_total"]
        .sum().round(2).reset_index().sort_values("annee_mois")
    )
    fig_m = px.line(
        par_mois, x="annee_mois", y="revenu_total", color="plateforme",
        color_discrete_map=COULEURS_PLATEFORME, markers=True,
        title="Évolution du revenu total par mois",
    )
    fig_m.update_traces(line=dict(width=2.5), marker=dict(size=7))
    fig_m.update_layout(xaxis_title=None, yaxis_title="Revenu ($)")
    col_b.plotly_chart(style_fig(fig_m, height=340), width="stretch")


with onglet_zones:
    st.markdown('<div class="section-title">Zones de prise en charge</div>',
                unsafe_allow_html=True)

    par_zone = (
        df.groupby("PULocationID")
        .agg(nombre_trajets=("revenu_total", "size"),
             revenu_total=("revenu_total", "sum"),
             revenu_moyen=("revenu_total", "mean"))
        .round(2).reset_index().rename(columns={"PULocationID": "zone_id"})
    )
    if zones is not None:
        par_zone = par_zone.merge(zones, on="zone_id", how="left")
        par_zone["libelle"] = (
            par_zone["zone_nom"].fillna("Zone " + par_zone["zone_id"].astype(str))
            + " (" + par_zone["arrondissement"].fillna("?") + ")"
        )
    else:
        par_zone["libelle"] = "Zone " + par_zone["zone_id"].astype(str)

    top = par_zone.sort_values("nombre_trajets", ascending=False).head(15)
    fig_z = px.bar(
        top.sort_values("nombre_trajets"), x="nombre_trajets", y="libelle",
        orientation="h", title="Top 15 des zones de départ (nombre de trajets)",
        text_auto=",.0f",
    )
    # Mesure de magnitude à identité unique : une seule teinte (slot bleu).
    fig_z.update_traces(marker_color="#2a78d6", textposition="outside",
                        textfont=dict(color=MUTED, size=11), cliponaxis=False,
                        hovertemplate="%{y}<br>%{x} trajets<extra></extra>")
    fig_z.update_layout(xaxis_title="Trajets", yaxis_title=None)
    st.plotly_chart(style_fig(fig_z, height=540, hovermode="closest"), width="stretch")

    st.dataframe(
        par_zone.sort_values("revenu_total", ascending=False)[
            ["zone_id", "libelle", "nombre_trajets", "revenu_total", "revenu_moyen"]
        ],
        width="stretch", hide_index=True,
        column_config={
            "zone_id": "Zone",
            "libelle": "Libellé",
            "nombre_trajets": st.column_config.NumberColumn("Trajets", format="%d"),
            "revenu_total": st.column_config.NumberColumn("Revenu total", format="%.2f $"),
            "revenu_moyen": st.column_config.NumberColumn("Revenu moyen", format="%.2f $"),
        },
    )


with onglet_donnees:
    st.markdown('<div class="section-title">Trajets filtrés</div>',
                unsafe_allow_html=True)
    st.caption(f"{fmt_nombre(len(df))} lignes correspondant aux filtres.")
    st.dataframe(df.head(2000), width="stretch", hide_index=True)
    st.download_button(
        "⬇️ Télécharger (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="trajets_filtres.csv",
        mime="text/csv",
        width="stretch",
    )
