# Dernière version des scripts python

## Download-req*csv.py

### Fonctionnement général

Ce script permet de télécharger les données directement dans pandas, puis de créer un Dataframe regroupant tous les attributs nécessaires afin de répondre à une des requêtes, et enfin d'exporter ce Dataframe en csv.

Il reste alors à effectuer une copie de ce fichier csv pour importer les données dans notre Cluster Casssandra.

### Lancement du script (exemple pour la requête 1)

Après avoir copié ce script sous le nom de download-req1csv.py sur une des VM, il faut lancer la commande suivante :
**$ python3 download-req1csv.py 20210101000000 20210115000000** pour obtenir un fichier csv contenant les données des 15 premiers jours du mois de Janvier 2021, réduit aux attributs importants pour la requête 1.

Une fois ce fichier généré (compter environ 15 minutes sur une machine), il faut lancer la commande suivante sur le cqlsh de la VM utilisée :
**> COPY table_1 ( attributs_dans_l'ordre_du_csv) FROM 'request1.csv' WITH DELIMITER = ',' AND HEADER=True;**

## MAIN.py

### Fonctionnement général

Le script python permet d'insérer dans une TABLE Cassandra toutes les données comprises entre deux dates choisies.

Afin de ne pas copier des données inutiles, un pré-traitement est effectué avant l'insertion de ces données.
Seuls les attributs qui sont nécessaires à la réalisation d'une requête seront gardés.

Seuls les requêtes 1 et 2 ont été traitées dans ce code.

*Note:* Pour la requête 1, une journée de données est insérer dans Cassandra en 4 minutes et 10 secondes.

Au contraire du script précédent, celui-ci permet de ne rien écrire sur disque, mais de ce fait l'insertion des données doit se faire ligne par ligne ce qui prends au final plus de temps. C'est pourquoi pour l'insertion finale des données, seuls les scripts download-req1csv.py et download-req2csv.py ont été utilisés.

Pour plus de détail, regarder les commentaires du script python.

### Lancement du script

Pour lancer le script Python entre le 1er Janvier 2021 et le 2 Janvier 2021, il faut utiliser la commande suivante:
**$ python3 MAIN.py 20210101000000 20210102000000**

### Axes d’améliorations possibles

1. Terminer les 2 autres requêtes, puis effectuer le même process que la fonction *request1*
2. Parallélisation du code sur l'ensemble des machines virtuelles
