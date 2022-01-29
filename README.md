# NoSQL_project

## Séance 28.01.2022

### Remarques :

- Utiliser python pour le téléchargement des données, le processing et les création de table sur Cassandra n’est pas la solution la plus optimisée à terme. Spark peut être une solution plus optimale.

### Répartition des tâches pour la suite du projet :

Différentes tâches peuvent être effectuées en parallèle pour avancer le plus efficacement possible sur le projet :

- Création d'un Keyspace, des tables et des requêtes sous Cassandra afin de répondre aux 4 questions posées. L’objectif est de créer une table par requête afin de tirer pleinnement profit de la technologie Cassandra.
    - Alvin, Urian
- Pre-processing des données sous Python. Création d’un script Python permettant le téléchargement des fichiers zip souhaités, le processing avec Pandas, puis l’écriture en csv afin que ces derniers puissent être directement chargés dans Cassandra. (Le pre-processing permet de réduire significativement la taille des données à importer dans Cassandra, ce qui permet un gain de temps important sur plusieurs milliers de fichiers).
    - Pré-requis : comprendre parfaitement l’architecture de la base de données afin de savoir où se trouve l’information pertinente pour chaque requête : table (EVENT, MENTIONS, GKG) et attribut.
    - Rayan, Waly, Alann
- Optimisation sur Spark : paralléliser le téléchargement et l’insertion des données avec Spark.
    - Valdez

### Etat d’avancement du projet :

- **CLUSTER :**

Le cluster Cassandra est fonctionnel sur 6 machines virtuelles, dont les id sont les suivants :

- Waly : tp-hadoop-13, 192.168.3.14
- Urian : tp-hadoop-3, 192.168.3.29
- Alann : tp-hadoop-20, 192.168.3.163
- Rayan : tp-hadoop-38, 192.168.3.84
- Valdez : tp-hadoop-45, 192.168.3.146
- Alvin : tp-hadoop-29, 192.168.3.160

L’installation et le déploiement du cluster Cassandra s’est faite par le biais de Docker.

Après installation de Docker sur chacune des machines virtuelles, nous nous sommes aidés du lien suivant pour la création et la mise en route du cluster : [https://hub.docker.com/_/cassandra](https://hub.docker.com/_/cassandra)

*Note* : il pourrait être utile de détailler toutes les commandes bash effectuées sur chacune des machines virtuelles.

- **REQUETES CQL :**

Pour effectuer les requêtes CQL, il faut être connecté sur une machine virtuelle, se placer dans le containeur actif avec Cassandra puis ouvrir le cqlsh (en s’assurant d’être dans le keyspace défini par Alvin : projetgdelt).

Pour cela, voici les commandes à effectuer :

**(base) alann@macbook-pro-de-goerke ~ %** ssh ubuntu@137.194.211.146

**ubuntu@tp-bridge-2**:**~**$ ssh tp-hadoop-20

**ubuntu@tp-hadoop-20**:**~**$ docker exec -it e111b20101bf bash

**root@tp-hadoop-20:/#** cqlsh

**cqlsh>** USE projetgdelt ;

**cqlsh:projetgdelt>** Possibilité de faire des requêtes dans cette console

Actuellement, seule la première requête est en cours.


- **SCRIPT PYTHON :**

J’ai créé deux fichiers :

- Un fichier Jupyter Notebook qui m’a permis de tester les fonctions codées,
- Un fichier python qui peut être lancé à partir d’une commande bash sur une VM, en remplaçant date-init et date-end par des str de la forme ‘YYYYMMDDHHMMSS’ (Exemple de date-init : ‘20210101000000’ pour le 01 Janvier 2021 à 00h 00min 00s)
    - python MAIN.py date-init date-end

**Récapitulatif des actions effectuées par le script python actuellement :**

Le script permet uniquement de post-traiter **un** fichier à partir d’une date donnée sous la forme explicitée précédement.

Le fonctionnement général du script est le suivant :

- Génération des 6 fichiers zip (export, mentions et gkq pour les articles en anglais et ceux en translingual),
- Téléchargement de ces fichiers (dans le répertoire où se trouve le scipt) avec la commande wget,
- Post-traitement, avec Pandas, de ces fichiers dans le but de répondre à la première requête,
- Ecriture sur la machine virtuelle d’un nouveau fichier “data_request1.csv” qui contient tous les attributs permettant de répondre à la requête 1 dans une seule table.

Pour plus de détail, regarder les commentaires du script python.

**Axes d’améliorations possibles :**

1. Générer tous les fichiers zip à télécharger pour **chacune** des dates entre les deux arguments (en prenant en compte un pas de temps de 15 minutes). Actuellement, le code traite uniquement un seul fichiuer de 15 minutes.
2. Parallélisation des téléchargements des fichiers sur les 6 machines du cluster pour gagner du temps.
