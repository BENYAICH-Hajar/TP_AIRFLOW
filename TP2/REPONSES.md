# TP Jour 2 — Logs E-Commerce avec HDFS

Ce TP a pour objectif de construire un pipeline ETL orchestré par Apache Airflow pour générer, stocker, analyser et archiver des logs e-commerce dans HDFS. Le pipeline comprend la génération de logs, l’upload dans HDFS, la vérification de présence du fichier, l’analyse du taux d’erreur, un branchement conditionnel selon ce taux, puis l’archivage final dans la zone processed.
## Q1 — HDFS vs système de fichiers local

Il ne faut pas simplement stocker les logs sur le disque local du serveur Airflow ou sur un NFS, car HDFS apporte plusieurs avantages importants dans un contexte de gros volumes de logs.

D’abord, HDFS est un système distribué. Cela veut dire que les données peuvent être réparties sur plusieurs machines. Dans un contexte de 50 Go de logs par jour, cela permet de mieux gérer la montée en charge.

Ensuite, HDFS offre la réplication des blocs. En production, un même bloc peut être dupliqué sur plusieurs DataNodes. Cela améliore la tolérance aux pannes : si un nœud tombe, les données restent disponibles.

Enfin, HDFS favorise la localité des données. Les moteurs de calcul comme Spark peuvent traiter les données au plus près de leur lieu de stockage, ce qui améliore les performances sur de gros traitements batch.
## Q2 — NameNode, point de défaillance unique

Dans HDFS, le NameNode gère les métadonnées : l’arborescence des fichiers, les emplacements des blocs et les droits d’accès. Si le NameNode tombe, les DataNodes conservent encore les blocs de données, mais les clients ne peuvent plus savoir où lire ou écrire. En pratique, le cluster devient inutilisable.

Pour éviter ce problème en production, Hadoop propose le mode HDFS HA (High Availability). Dans ce mode, on utilise deux NameNodes : un actif et un standby. Si le NameNode actif tombe, le standby peut prendre le relais.

Le JournalNode joue un rôle important dans cette architecture. Il permet de synchroniser les journaux d’édition entre les NameNodes pour que le standby soit toujours à jour.
## Q3 — HdfsSensor en mode poke vs reschedule

Le mode poke garde le worker occupé pendant toute la durée d’attente. Cela peut bloquer inutilement des ressources si le fichier met du temps à arriver.

Le mode reschedule libère le worker entre deux vérifications. C’est plus adapté en production, surtout avec CeleryExecutor, car cela évite de monopoliser un slot pendant une longue attente.

J’utiliserais le mode poke pour une attente très courte et prévisible. J’utiliserais le mode reschedule pour une attente potentiellement longue ou variable.

Un mauvais choix peut bloquer tout le scheduler. Par exemple, si plusieurs sensors restent longtemps en mode poke en attendant des fichiers externes, ils peuvent occuper tous les workers et empêcher l’exécution des autres tâches du pipeline.
## Q4 — Réplication HDFS et cohérence des données

Dans ce TP, le facteur de réplication est fixé à 1, ce qui est suffisant pour un environnement local. En production, avec un facteur de réplication de 3, chaque bloc de 128 Mo est écrit en trois copies sur trois DataNodes différents.

L’écriture se fait en pipeline : le client envoie le bloc au premier DataNode, qui le transmet au second, puis au troisième. Cela garantit que les répliques sont créées de manière coordonnée.

En lecture, HDFS garantit que les clients ne voient que des blocs valides et cohérents. Pendant qu’une écriture est en cours, les données ne sont pas considérées comme complètement disponibles tant qu’elles ne sont pas correctement finalisées.
## Conclusion

Ce TP m’a permis de comprendre comment construire un pipeline ETL complet avec Apache Airflow et HDFS. J’ai pu mettre en place la génération de logs, leur stockage dans HDFS, leur analyse automatique, un branchement conditionnel selon le taux d’erreur, puis leur archivage final. La principale difficulté rencontrée concernait l’accès à HDFS depuis le conteneur Airflow. Cette difficulté a été résolue en utilisant WebHDFS au lieu de commandes Docker directes dans le DAG. Finalement, le pipeline a pu être exécuté correctement de bout en bout.