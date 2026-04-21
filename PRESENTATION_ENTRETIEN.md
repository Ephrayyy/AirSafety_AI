# Presentation entretien

## Positionnement du projet

Ce projet est une maquette de pipeline data inspiree des missions de l'offre DGAC :

- acquisition de donnees via API en Python
- ETL et preparation pour la visualisation
- enrichissement NLP sur des comptes rendus d'incidents
- suivi automatique d'indicateurs
- detection semi-automatisee d'incidents atypiques

## Pitch de 90 secondes

"Pour preparer l'entretien, j'ai construit une mini-chaine data orientee securite aerienne. Je pars de deux sources de donnees simulees de type API : des signalements d'incidents et un contexte de vol. Ensuite, j'applique un ETL Python pour nettoyer, typer et fusionner les informations. J'ajoute une brique NLP simple pour extraire des themes a partir des descriptions textuelles, puis je calcule un score de risque composite et je detecte les incidents atypiques. Enfin, j'expose les sorties dans une interface Streamlit, ce qui permet d'imaginer facilement une integration vers Power BI. Ce projet me permet de montrer a la fois mes bases en ingenierie des donnees, mon aisance en Python et ma capacite a relier technique et besoin metier."

## Ce que tu dois montrer pendant la demo

1. `data/raw/incident_reports.json` et `data/raw/flight_context.json`
   Montrer que tu raisonnes en sources heterogenes et en ingestion.

2. `src/airsafety_ai/pipeline.py`
   Montrer la logique ETL : jointure, normalisation, enrichissement, export.

3. `topic_tags`, `composite_risk_score`, `is_atypical`
   Montrer que tu sais transformer un texte libre en information exploitable.

4. `data/processed/safety_indicators.csv`
   Montrer le suivi automatique d'indicateurs.

5. `app.py`
   Montrer la restitution metier et la priorisation des cas a analyser.

## Qualites a rattacher explicitement a l'offre

- Python : pipeline clair, reproductible, modulaire.
- ETL : nettoyage, jointure, typage et export analytique.
- Visualisation : tableau de bord simple et lisible.
- NLP / LLM : base NLP interpretable, extensible vers un modele plus avance.
- Detection atypique : approche pragmatique avec score de risque + Isolation Forest.
- Logique metier : securite aerienne, priorisation, aide a l'analyse.

## Questions probables et reponses courtes

### Pourquoi avoir choisi ce projet ?

"Parce qu'il couvre en version compacte l'ensemble des briques mentionnees dans l'offre, avec un cas d'usage proche de la securite aerienne."

### Pourquoi ne pas avoir utilise un vrai LLM ?

"Pour une demo rapide, j'ai privilegie une brique NLP interpretable. L'architecture est faite pour pouvoir remplacer cette partie par un modele plus avance ou un LLM ensuite."

### Comment industrialiser ce projet ?

"Je separerais ingestion, transformation et serving dans des jobs planifies, avec validation de schema, supervision, tests de qualite de donnees et exposition vers un outil BI."

### Comment brancher Power BI ?

"Les sorties `incident_master.csv` et `safety_indicators.csv` sont deja dans un format tabulaire adapte pour une connexion BI."

### Quelle est la limite principale de cette maquette ?

"La source est simulee et les regles NLP sont simples. La valeur du projet est surtout de montrer la chaine technique et ma facon de structurer un besoin data."

## Ce qu'il faut dire si on te demande ton apport personnel

"J'ai voulu montrer que je ne vois pas la data seulement comme de l'analyse, mais comme une chaine complete : acquisition, fiabilisation, enrichissement, restitution et aide a la decision."
