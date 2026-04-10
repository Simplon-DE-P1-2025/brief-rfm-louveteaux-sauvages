import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="RFM Documentation - Over The Top",
    layout="wide",
)

html = """
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css" />
  <style>
    :root {
      --txt: #edf2ff;
      --muted: #bcc8ff;
      --glass: rgba(8, 14, 36, 0.72);
      --border: rgba(255, 255, 255, 0.14);
      --pink: #ff4fd8;
      --cyan: #64f9ff;
      --gold: #ffd86b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Manrope", Arial, sans-serif;
      color: var(--txt);
      background:
        radial-gradient(circle at 12% 10%, rgba(255, 79, 216, 0.24), transparent 33%),
        radial-gradient(circle at 88% 16%, rgba(100, 249, 255, 0.2), transparent 35%),
        radial-gradient(circle at 52% 90%, rgba(255, 216, 107, 0.18), transparent 42%),
        linear-gradient(140deg, #090f22 0%, #151f42 40%, #220f3f 100%);
      min-height: 100vh;
      padding: 18px;
    }
    .wrap {
      max-width: 1200px;
      margin: 0 auto;
    }
    .hero {
      border: 1px solid var(--border);
      border-radius: 28px;
      padding: 30px 34px;
      background: linear-gradient(160deg, rgba(255, 255, 255, 0.10), rgba(255, 255, 255, 0.02));
      box-shadow: 0 24px 60px rgba(0, 0, 0, 0.35), 0 0 44px rgba(100, 249, 255, 0.14);
      backdrop-filter: blur(8px);
      overflow: hidden;
      position: relative;
    }
    .hero::before {
      content: "";
      position: absolute;
      inset: -150px auto auto -120px;
      width: 350px;
      height: 350px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(255, 79, 216, 0.25), transparent 70%);
      animation: spin 13s linear infinite;
    }
    .hero-title {
      font-size: clamp(2.1rem, 5vw, 3.9rem);
      font-weight: 800;
      line-height: 1.02;
      margin: 0 0 4px;
      background: linear-gradient(90deg, var(--pink), var(--cyan), var(--gold));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      text-shadow: 0 10px 24px rgba(100, 249, 255, 0.28);
      position: relative;
      z-index: 2;
    }
    .hero-subtitle {
      margin: 0;
      font-size: 1.12rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      position: relative;
      z-index: 2;
    }
    .card {
      margin-top: 16px;
      border: 1px solid var(--border);
      border-radius: 24px;
      padding: 24px 28px;
      background: var(--glass);
      box-shadow: 0 18px 48px rgba(0, 0, 0, 0.35);
    }
    .accent {
      height: 5px;
      border-radius: 999px;
      margin-bottom: 18px;
      background: linear-gradient(90deg, var(--pink), var(--cyan), var(--gold));
    }
    .block {
      margin-bottom: 20px;
      padding: 16px 18px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.03);
    }
    h2 {
      margin: 0 0 10px;
      font-size: 1.55rem;
      color: #ffffff;
    }
    h3 {
      margin: 10px 0 8px;
      font-size: 1.15rem;
      color: #e6f9ff;
    }
    p, li {
      font-size: 1.01rem;
      line-height: 1.62;
      color: #e9eeff;
    }
    ul { margin: 8px 0 0 20px; }
    .quote {
      margin-top: 8px;
      padding: 10px 12px;
      border-left: 3px solid var(--cyan);
      border-radius: 8px;
      background: rgba(100, 249, 255, 0.08);
      color: #dffcff;
    }
    a {
      color: var(--cyan);
      text-decoration: none;
      border-bottom: 1px dashed rgba(100, 249, 255, 0.5);
    }
    a:hover {
      color: #ffffff;
      border-bottom-color: #ffffff;
    }
    @keyframes spin {
      from { transform: rotate(0deg); }
      to { transform: rotate(360deg); }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero animate__animated animate__zoomIn animate__faster">
      <h1 class="hero-title animate__animated animate__jackInTheBox animate__delay-1s">Documentation RFM</h1>
      <p class="hero-subtitle animate__animated animate__fadeInUp animate__delay-1s">brief-rfm-louveteaux-sauvages</p>
    </section>

    <section class="card animate__animated animate__fadeInUp animate__delay-1s">
      <div class="accent animate__animated animate__pulse animate__infinite animate__slow"></div>

      <div class="block animate__animated animate__fadeInUp animate__delay-1s">
        <h2>Brief Projet : Pipeline Data RFM (Docker & Airflow)</h2>
      </div>

      <div class="block animate__animated animate__fadeInLeft animate__delay-2s">
        <h2>Contexte</h2>
        <p>
          Ce projet vise a construire une pipeline de donnees simple et fonctionnelle a partir d'un dataset RFM.
          L'objectif n'est pas de creer une architecture complete, mais de mettre en place une pipeline orchestree et dockerisee.
        </p>
      </div>

      <div class="block animate__animated animate__fadeInRight animate__delay-2s">
        <h2>Objectifs pedagogiques</h2>
        <ul>
          <li>Mettre en place une pipeline de donnees simple</li>
          <li>Comprendre l'orchestration avec Airflow</li>
          <li>Comprendre la dockerisation d'un projet data</li>
          <li>Faire fonctionner plusieurs services ensemble (PostgreSQL, Airflow, ETL)</li>
        </ul>
      </div>

      <div class="block animate__animated animate__fadeInLeft animate__delay-3s">
        <h2>Perimetre du projet</h2>
        <p>Le projet se concentre uniquement sur :</p>
        <ul>
          <li>L'ingestion des donnees</li>
          <li>Une transformation simple (calcul RFM)</li>
          <li>L'orchestration avec Airflow</li>
          <li>La dockerisation avec <code>docker-compose</code></li>
        </ul>
        <p class="quote">La visualisation est optionnelle.</p>
      </div>

      <div class="block animate__animated animate__fadeInRight animate__delay-3s">
        <h2>Taches a realiser</h2>
        <h3>Etape 1 - Ingestion</h3>
        <ul><li>Charger les donnees dans PostgreSQL (table brute)</li></ul>
        <h3>Etape 2 - Transformation</h3>
        <ul>
          <li>Nettoyer les donnees</li>
          <li>Calculer Recency, Frequency, Monetary</li>
        </ul>
        <h3>Etape 3 - Orchestration (Airflow)</h3>
        <ul><li>Creer un DAG avec 3 taches : ingestion, transformation, chargement</li></ul>
        <h3>Etape 4 - Dockerisation</h3>
        <ul>
          <li>Mettre en place un <code>docker-compose</code> avec PostgreSQL, Airflow, et script ETL</li>
        </ul>
        <h3>Etape 5 - (Optionnel) Visualisation</h3>
        <ul><li>Utiliser Streamlit ou toute autre solution si souhaite</li></ul>
      </div>

      <div class="block animate__animated animate__fadeInLeft animate__delay-4s">
        <h2>Ressources</h2>
        <ul>
          <li>Dataset : <a href="https://archive.ics.uci.edu/dataset/502/online+retail+ii" target="_blank">UCI Online Retail II</a></li>
          <li>Repo Docker/Airflow : <a href="https://github.com/gsoulat/formation-data-engineer" target="_blank">formation-data-engineer</a></li>
        </ul>
      </div>

      <div class="block animate__animated animate__fadeInUp animate__delay-4s">
        <h2>Modalites de travail</h2>
        <ul>
          <li>Travail en groupes de 3</li>
          <li>Approche pratique : learning by doing</li>
          <li>Le projet commence immediatement</li>
        </ul>
      </div>

      <div class="block animate__animated animate__fadeInUp animate__delay-4s">
        <h2>Livrables attendus</h2>
        <ul>
          <li><code>docker-compose.yml</code></li>
          <li>DAG Airflow</li>
          <li>Scripts ETL</li>
          <li>README simple pour execution</li>
        </ul>
      </div>

      <div class="block animate__animated animate__fadeInUp animate__delay-4s">
        <h2>Presentation finale</h2>
        <p>
          Chaque groupe devra realiser une presentation orale (10 a 15 minutes maximum). La presentation doit inclure
          une demonstration du projet, une explication claire du fonctionnement, une justification des choix techniques,
          et une preuve de comprehension des concepts utilises.
        </p>
        <p>
          L'objectif n'est pas de faire des slides complexes, mais de montrer que le projet fonctionne et que vous
          comprenez ce que vous avez realise.
        </p>
      </div>

      <div class="block animate__animated animate__fadeInUp animate__delay-4s">
        <h2>Evaluation</h2>
        <ul>
          <li>Fonctionnement de la pipeline</li>
          <li>Orchestration correcte</li>
          <li>Docker fonctionnel</li>
          <li>Simplicite et clarte</li>
          <li>Capacite a expliquer le projet</li>
        </ul>
      </div>
    </section>
  </div>
</body>
</html>
"""

components.html(html, height=2300, scrolling=True)
