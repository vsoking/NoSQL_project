# Dernière version du script python

## Fonctionnement général

Le script python permet d'insérer dans une TABLE Cassandra toutes les données comprises entre deux dates choisies.

Afin de ne pas copier des données inutiles, un pré-traitement est effectué avant l'insertion de ces données.
Seuls les attributs qui sont nécessaires à la réalisation d'une requête seront gardés.

Pour le moment, seule la requête 1 à été traitée.

*Note:* Pour la requête 1, une journée de données est insérer dans Cassandra en 4 minutes et 10 secondes.

Pour plus de détail, regarder les commentaires du script python.

## Lancement du script

Pour lancer le script Python entre le 1er Janvier 2021 et le 2 Janvier 2021, il faut utiliser la commande suivante:
**$ python3 MAIN.py 20210101000000 20210102000000**

## Axes d’améliorations possibles

1. Terminer les 3 autres requêtes, puis effectuer le même process que la fonction *request1*
2. Parallélisation du code sur l'ensemble des machines virtuelles
