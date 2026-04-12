# LocalExecutor est adapté au développement local,
# PostgreSQL est utilisé pour stocker les métadonnées Airflow,
# les volumes permettent la persistance et la modification des DAGs sans rebuild.

# REPONSES.md

## TP Jour 1 — Pipeline Énergie & Météo avec Docker

### Q1 — Différence entre LocalExecutor, CeleryExecutor et KubernetesExecutor

Dans ce TP, le fichier `docker-compose.yaml` utilise **LocalExecutor**.  
Ce mode d’exécution permet de lancer plusieurs tâches en parallèle sur une seule machine. Il est adapté au développement local, aux tests et aux petits projets. Il est simple à mettre en place car il ne nécessite pas de broker de messages supplémentaire.

**CeleryExecutor** fonctionne différemment. Il distribue les tâches sur plusieurs workers à l’aide d’un broker comme Redis ou RabbitMQ. Ce mode est plus adapté à une production avec une charge importante, car il permet une vraie scalabilité horizontale. En revanche, il est plus complexe à administrer.

**KubernetesExecutor** lance chaque tâche dans un pod Kubernetes séparé. Cela permet une meilleure isolation, une gestion plus flexible des ressources et une très bonne intégration avec une infrastructure cloud-native. C’est une solution très intéressante pour les grands environnements de production, mais elle demande un cluster Kubernetes et une mise en place plus avancée.

Pour un contexte RTE :
- **LocalExecutor** convient pour le développement local, les tests et les maquettes.
- **CeleryExecutor** serait utile pour une plateforme interne avec plusieurs pipelines Airflow et une charge régulière.
- **KubernetesExecutor** serait pertinent en production à grande échelle, surtout si l’infrastructure est déjà basée sur Kubernetes.

En termes de scalabilité :
- LocalExecutor : limité à une seule machine
- CeleryExecutor : scalable horizontalement avec plusieurs workers
- KubernetesExecutor : très scalable, avec allocation dynamique des ressources

---

### Q2 — Volumes Docker et persistance des DAGs

Le mapping `./dags:/opt/airflow/dags` est un **bind mount**.  
Cela signifie que le dossier `dags` de ma machine locale est directement lié au dossier `dags` dans le conteneur Airflow.

L’avantage est que lorsqu’on modifie un fichier DAG sur la machine, Airflow peut le voir presque immédiatement dans le conteneur, sans reconstruire l’image Docker. Cela est très pratique en développement, car on peut tester rapidement les modifications.

Si on supprimait ce mapping :
- le conteneur ne verrait plus les fichiers DAG présents sur la machine
- les nouveaux DAGs ne seraient pas détectés
- il faudrait reconstruire l’image ou copier manuellement les fichiers dans le conteneur

La différence entre un **bind mount** et un **volume nommé** est la suivante :
- un bind mount pointe vers un dossier réel du système hôte
- un volume nommé est géré par Docker lui-même dans son propre espace de stockage

En production, sur un cluster Airflow avec plusieurs workers, cette question est très importante. Tous les nœuds doivent pouvoir accéder aux mêmes DAGs. Sinon, certains workers pourraient exécuter une version différente du code, ou ne pas trouver le DAG du tout. Dans ce cas, on utilise souvent :
- un stockage partagé
- une image Docker commune
- ou une synchronisation centralisée des DAGs

---

### Q3 — Catchup et idempotence

Dans ce TP, le DAG a `catchup=False`.  
Cela signifie qu’Airflow ne va pas exécuter automatiquement tous les anciens runs non joués depuis la `start_date`.

Si `catchup=True` et que le DAG est activé aujourd’hui avec une `start_date` au 1er janvier 2024, Airflow essaierait de créer toutes les exécutions quotidiennes manquantes entre cette date et aujourd’hui. Cela pourrait produire un grand nombre de runs inutiles ou non désirés.

L’**idempotence** signifie qu’un DAG ou une tâche peut être rejoué plusieurs fois sans produire d’effets incohérents ou de doublons. C’est une propriété très importante dans les pipelines de données.

Dans un pipeline énergétique, c’est critique car :
- on veut éviter les doublons de rapports
- on veut éviter d’écraser des données valides par erreur
- on veut pouvoir relancer un traitement sans casser les résultats

Pour rendre les fonctions `collecter_*` idempotentes, on peut :
- toujours produire un résultat calculé à partir des données sources du moment
- éviter d’insérer plusieurs fois les mêmes données dans une base sans contrôle
- utiliser des fichiers de sortie nommés de manière déterministe
- remplacer un rapport existant au lieu d’en créer plusieurs copies incohérentes

Dans ce TP, l’écriture du rapport JSON avec un nom basé sur la date va déjà dans ce sens.

---

### Q4 — Importance de la timezone Europe/Paris

Le paramètre `timezone=Europe/Paris` est essentiel dans ce TP, car les données météo et les données énergétiques doivent être interprétées dans le même fuseau horaire que le contexte métier de RTE.

Si la timezone n’est pas bien gérée :
- les données météo peuvent correspondre à une heure différente des données éCO2mix
- les comparaisons deviennent fausses
- une anomalie peut être détectée à tort

Le passage à l’heure d’été est particulièrement sensible. Par exemple, lors du dernier dimanche de mars, une heure disparaît. Si le scheduler Airflow ou les requêtes API ne gèrent pas correctement ce changement, on peut avoir :
- une heure manquante
- une heure dupliquée
- un décalage entre les mesures météo et les données de production

Exemple concret :
si la météo est récupérée avec un fuseau UTC mais que la production est interprétée en heure de Paris, on peut comparer un ensoleillement de 14h avec une production électrique de 13h. Le rapport peut alors conclure à tort qu’il y a une anomalie de production, alors qu’il s’agit simplement d’un décalage horaire.

C’est pour cela qu’il est important d’utiliser la même timezone dans :
- le DAG Airflow
- les appels API
- l’analyse métier

---

## Captures d’écran à inclure

### 1. Interface Airflow avec le DAG `energie_meteo_dag` en succès
Capture montrant le DAG visible et les runs réussis.

### 2. Vue Graph du DAG
Capture montrant les 5 tâches :
- `verifier_apis`
- `collecter_meteo_regions`
- `collecter_production_electrique`
- `analyser_correlation`
- `generer_rapport_energie`

### 3. Logs de `generer_rapport_energie`
Capture montrant le tableau affiché dans les logs Airflow.

### 4. Onglet XCom de `analyser_correlation`
Capture montrant le dictionnaire d’alertes avec les statuts des régions.

### 5. Contenu du fichier JSON généré
Capture du contenu de `/tmp/rapport_energie_2026-04-12.json`

---

## Conclusion

Dans ce TP, j’ai mis en place un pipeline Airflow local avec Docker permettant de collecter des données météo et de production électrique, puis de les corréler afin de détecter des alertes. J’ai utilisé un DAG composé de cinq tâches avec transmission des données intermédiaires via XCom. Le pipeline génère enfin un rapport JSON exploitable. Ce TP m’a permis de mieux comprendre l’orchestration avec Airflow, la structure d’un DAG, l’intérêt des dépendances, ainsi que les bonnes pratiques liées à Docker, au catchup, à l’idempotence et à la gestion des fuseaux horaires.