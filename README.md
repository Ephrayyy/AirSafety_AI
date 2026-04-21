# AirSafety AI Demo

Projet de demonstration pour un entretien autour d'une alternance en ingenierie des donnees a la DGAC.

Le projet simule une chaine simple et presentable :

- acquisition de donnees issues de faux flux API "incident reports" et "flight context"
- pre-traitement ETL en Python
- enrichissement NLP sur des descriptions textuelles d'incidents
- calcul d'indicateurs de securite
- detection d'incidents atypiques
- restitution dans une application Streamlit

## Pourquoi ce projet correspond a l'offre

L'offre mentionne notamment :

- acquisition de donnees par API en Python
- pre-traitement ETL
- visualisation de donnees
- NLP / LLM
- suivi automatique d'indicateurs
- classification semi-automatisee d'incidents atypiques

Ce depot couvre chacun de ces points avec une version compacte, explicable et montrable rapidement.

## Structure

- `src/airsafety_ai/mock_data.py` : generation d'un jeu de donnees reproductible
- `src/airsafety_ai/pipeline.py` : ingestion, ETL, enrichissement et calcul des indicateurs
- `app.py` : application Streamlit
- `data/raw/` : donnees simulees de type API
- `data/processed/` : sorties du pipeline

## Lancer la demo

```bash
python3 -m src.airsafety_ai.pipeline
streamlit run app.py
```

## Script de demo pour l'entretien

1. Expliquer le besoin metier : centraliser des signalements securite et les rendre exploitables.
2. Montrer la generation ou l'ingestion de donnees brutes.
3. Expliquer l'ETL : normalisation des statuts, typage des dates, jointure contexte vol + incident.
4. Montrer l'enrichissement texte : themes detectes et score de risque textuel.
5. Montrer la detection d'incidents atypiques et les indicateurs.
6. Ouvrir Streamlit pour filtrer les incidents et visualiser les alertes prioritaires.

## Points a mettre en avant oralement

- Le projet est volontairement simple mais structure comme un vrai mini-produit data.
- La logique est reproductible et separable en briques industrialisables.
- Le composant NLP est concu pour etre remplace ensuite par un modele plus avance ou un LLM.
- Les sorties tabulaires peuvent etre alimentees dans Power BI ou un autre outil BI.

## Pitch court

"J'ai prepare une maquette de pipeline securite aerienne. Je simule deux sources de donnees de type API, je les integre dans un ETL Python, j'enrichis les descriptions d'incidents avec une couche NLP, je calcule des indicateurs de suivi et je detecte les evenements atypiques. L'objectif etait de montrer que je sais relier acquisition, qualite de donnees, analyse et restitution dans une logique proche de vos missions."
