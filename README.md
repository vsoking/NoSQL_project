# NoSQL_project

## Groupe

Notre groupe de travail est composé de 6 personnes :

- Waly : tp-hadoop-13, 192.168.3.14
- Urian : tp-hadoop-3, 192.168.3.29
- Alann : tp-hadoop-20, 192.168.3.163
- Rayan : tp-hadoop-38, 192.168.3.84
- Valdez : tp-hadoop-45, 192.168.3.146
- Alvin : tp-hadoop-29, 192.168.3.160

## Description des dossiers

Sur ce repository, vous trouverez 4 dossiers :

- Avancement :

La planification des actions à effectuer, mise à jour lors des séances.

- Documentation :

Une description de tous les attributs des 3 tables étudiées ainsi que les fichiers explicatifs pdf en anglais.
Cette section permet de retravailler les 3 tables afin de mieux comprendre leurs attributs.  

- Scripts_python
Plusieurs versions des scripts pythons qui ont permis de lancer le téléchargement des fichiers, de manipuler ces fichiers et d'insérer les données dans Cassandra pour répondre aux 4 requêtes demandées.

- spark_job :
Application spark, qui télécharge la donnée , effectue les traitements nécessaires puis insère la donnée dans cassandra.  Cette solution n'a pas été utilisée au final car trop lente, et au bout d'un moment l'application finit par crash car nous n'avons pas assez de ressources (RAM) pour exécuter cette application. Une optimisation est nécessaire.
