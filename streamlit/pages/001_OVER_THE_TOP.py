import streamlit as st

st.set_page_config(
    page_title="RFM Documentation - Over The Top",
    layout="wide",
)

st.markdown(
    """
    <link
        rel="stylesheet"
        href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css"
    />
    <style>
    .stApp {
        background:
            radial-gradient(circle at 10% 15%, rgba(255, 0, 170, 0.20), transparent 35%),
            radial-gradient(circle at 90% 20%, rgba(0, 255, 245, 0.20), transparent 35%),
            radial-gradient(circle at 50% 85%, rgba(255, 200, 0, 0.16), transparent 40%),
            linear-gradient(135deg, #0b1020 0%, #141b34 45%, #1f1236 100%);
        color: #f8f9ff;
    }

    .hero-wrap {
        border: 1px solid rgba(255, 255, 255, 0.15);
        background: linear-gradient(
            145deg,
            rgba(255, 255, 255, 0.10),
            rgba(255, 255, 255, 0.03)
        );
        border-radius: 24px;
        padding: 2rem 2.4rem;
        backdrop-filter: blur(8px);
        box-shadow:
            0 20px 50px rgba(0, 0, 0, 0.35),
            0 0 40px rgba(0, 224, 255, 0.12);
        margin-bottom: 1.5rem;
    }

    .title-art {
        font-size: clamp(2rem, 5vw, 3.4rem);
        font-weight: 900;
        line-height: 1.05;
        margin-bottom: .25rem;
        background: linear-gradient(90deg, #ff4fd8, #64f9ff, #ffd86b);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-shadow: 0 6px 24px rgba(100, 249, 255, 0.30);
    }

    .subtitle-art {
        font-size: 1.05rem;
        letter-spacing: 0.04em;
        opacity: 0.92;
        margin-bottom: 0;
    }

    .content-card {
        border: 1px solid rgba(255, 255, 255, 0.12);
        background: rgba(9, 14, 30, 0.72);
        border-radius: 20px;
        padding: 1.2rem 1.6rem;
        box-shadow: 0 16px 35px rgba(0, 0, 0, 0.28);
    }

    .accent-line {
        height: 4px;
        width: 100%;
        border-radius: 999px;
        background: linear-gradient(90deg, #ff4fd8, #64f9ff, #ffd86b);
        margin: 0.2rem 0 1.2rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero-wrap animate__animated animate__fadeInDown">
        <div class="title-art">Documentation RFM</div>
        <p class="subtitle-art">brief-rfm-louveteaux-sauvages</p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="content-card animate__animated animate__fadeInUp animate__delay-1s"><div class="accent-line"></div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
# Brief Projet : Pipeline Data RFM (Docker & Airflow)
## Contexte
Ce projet vise à construire une pipeline de donnees simple et fonctionnelle a partir d'un dataset RFM.  
L'objectif n'est pas de creer une architecture complete, mais de mettre en place une pipeline orchestree et dockerisee.

## Objectifs pedagogiques
- Mettre en place une pipeline de donnees simple
- Comprendre l'orchestration avec Airflow
- Comprendre la dockerisation d'un projet data
- Faire fonctionner plusieurs services ensemble (PostgreSQL, Airflow, ETL)

## Perimetre du projet
Le projet se concentre uniquement sur :
- L'ingestion des donnees
- Une transformation simple (calcul RFM)
- L'orchestration avec Airflow
- La dockerisation avec `docker-compose`
> La visualisation est optionnelle.

## Taches a realiser
### Etape 1 - Ingestion
- Charger les donnees dans PostgreSQL (table brute)

### Etape 2 - Transformation
- Nettoyer les donnees
- Calculer Recency, Frequency, Monetary

### Etape 3 - Orchestration (Airflow)
- Creer un DAG avec 3 taches : ingestion, transformation, chargement

### Etape 4 - Dockerisation
- Mettre en place un `docker-compose` avec :
  - PostgreSQL
  - Airflow
  - Script ETL

### Etape 5 - (Optionnel) Visualisation
- Utiliser Streamlit ou toute autre solution si souhaite

## Ressources
- Dataset : [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
- Repo Docker/Airflow : [formation-data-engineer](https://github.com/gsoulat/formation-data-engineer)

## Modalites de travail
- Travail en groupes de 3
- Approche pratique : learning by doing
- Le projet commence immediatement

## Livrables attendus
- `docker-compose.yml`
- DAG Airflow
- Scripts ETL
- README simple pour execution

## Presentation finale
Chaque groupe devra realiser une presentation orale (10 a 15 minutes maximum).
La presentation doit inclure :
- Une demonstration du projet (pipeline, Docker, Airflow)
- Une explication simple et claire du fonctionnement
- Une justification des choix techniques
- Une preuve de comprehension des concepts utilises

L'objectif n'est pas de faire des slides complexes, mais de montrer que le projet fonctionne et que vous comprenez ce que vous avez realise.

## Evaluation
- Fonctionnement de la pipeline
- Orchestration correcte
- Docker fonctionnel
- Simplicite et clarte
- Capacite a expliquer le projet
""",
)

st.markdown("</div>", unsafe_allow_html=True)
