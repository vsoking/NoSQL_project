# NoSQL_project

## Groupe

Notre groupe de travail est composé de 6 machines :

-   tp-hadoop-13
-   tp-hadoop-3, 
-   tp-hadoop-20, 
-   tp-hadoop-38, 
-   tp-hadoop-45, 
-   tp-hadoop-29, 

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
