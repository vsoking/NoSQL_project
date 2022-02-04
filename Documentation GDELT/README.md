# Analyse des attributs présents dans les 3 tables

Les 3 tables à étudier sont : EVENT, MENTIONS et GKG.


## Table EVENT (246,16 Go)

Dans cette table, il y a 5 catégories d’attributs :

1. Les dates et évènements,
2. Les acteurs,
3. Les actions des évènements,
4. Leur géographie,
5. La gestion des champs de données.

*Remarque* : « To examine events at the 15 minute resolution, use the DATEADDED field (the second from the last field in this table at the end). » (p.2)
=========================================================
### 1. Les dates et évènements

- GlobalEventID : Identifiant unique assigné à chaque évènement enregistré.
- Day : Date à laquelle l’évènement à eu lieu, au format YYYYMMDD.
- MonthYear : Date alternative au format : YYYYMM.
- Year : Date alternative au format : YYYY.
- FractionDate :  Date alternative au format : YYYY.FFFF avec FFFF le pourcentage de l’année complétée le jour de l’évènement (ce pourcentage va de 0 à 0.9999% pour capturer 965 jours). Le calcul est effectué de la sorte : (MONTH - 30 + DAY ) / 365.

### 2. Les acteurs

- Actor1Code : Code CAMEO complet pour l’acteur 1. Si celui-ci est blanc, c’est que l’acteur1 n’est pas identifié.
- Actor1Name : Nom de l’acteur 1.
- Actor1CountryCode : Code de 3 caractères désignant le pays de l’acteur 1.
- Actor1KnownGroupCode : Contient le code de l’organisation de l’acteur 1 si celui-ci fait partie d’une organisation du type IGO/NGO/rebel.
- Actor1EthnicCode : Code de l’affiliation ethnique de l’acteur 1, si celui-ci est indiqué.
- Actor1Religion1Code : Code de l’affiliation religieuse de l’acteur 1, si celui-ci est indiqué.
- Actor1Religion2Code : Idem que 1 si l’acteur 1 a plusieurs religions.
- Actor1Type1Code : Code de 3 caractères indiquant le type ou le rôle de l’acteur 1 si celui-ci est spécifié (exemple: Police forces, Government..)
- Actor1Type2Code : Dans le cas où l’acteur 1 a plusieurs roles/types.
- Actor1Type3Code : Dans le cas où l’acteur 1 a plusieurs roles/types.

Tous les attributs ci-dessus sont répétés pour l’acteur 2, en remplaçant les noms par Actor2.

### 3. Les actions des évènements

Ces attributs permettent de répondre à la question : Qu’a fait l’acteur 1 à l’acteur 2 ?

- IsRootEvent : Permet de connaître l’importance d’un évènement, de savoir s’il y a une forte chance qu’il créé d’autres évènements en lien / qui découlent de celui-ci.
- EventCode : Code CAMEO décrivant l’action de l’acteur 1 sur l’acteur 2
- EventBaseCode : Les évènements CAMEO sont définis sur 3 niveaux. Pour un événement de niveaux 3 (ex: ‘0251’), cet attribut correspond au niveau 2 au dessus (ex: ‘025’).
- EventRootCode : Défini le niveau 1, le niveau racine (ex: ’02’).
- QuadClass : Représente le niveau de classification primaire d’un évènement : 1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, and 4=Material Conflict. (Seuls ces 4 niveaux sont définis)
- GoldsteinScale : Score numérique compris entre -10 et +10, représentant l’impact potentiel qu’un certain type d’évènement va avoir sur la stabilité d’un pays.
- NumMentions : Nombre total de mentions d’un évènement parmi tous les documents sources pendant les 15 minutes durant lesquelles il a été vu pour la première fois. (Si un même document référence plusieurs fois cet évènement, cela va être compté également)
- NumSources : Nombre total de sources d’informations qui contiennent une ou plusieurs mentions de cette évènement pendant les 15 minutes durant lesquelles il a été vu pour la première fois.
- NumArticles : Nombre total de documents sources qui contiennent une ou plusieurs mentions de cette évènement pendant les 15 minutes durant lesquelles il a été vu pour la première fois.
- AvgTone : La tonalité moyenne de tous les documents qui contiennent une ou plusieurs mentions de cette évènement pendant les 15 minutes durant lesquelles il a été vu pour la première fois. C’est un score compris entre -100 et +100

### 4. Leur géographie

- Actor1Geo_Type : Spécifie la résolution géographique : 1=COUNTRY, 2=USSTATE, 3=USCITY, 4=WORLDCITY, 5= WORLDSTATE.
- Actor1Geo_Fullname : Le nom complet de la localisation de l’acteur 1.
- Actor1Geo_CountryCode : Le code du pays en 2 caractère : FIPS10-4.
- Actor1Geo_ADM1Code : Le code du pays en 2 caractères suivi du code de division administrative 1 en 2 caractères également.
- Actor1Geo_ADM2Code :  Le code du pays en 2 caractères suivi du code de division administrative 2 en 2 caractères également.
- Actor1Geo_Lat : Latitude.
- Actor1Geo_Long : Longitude.
- Actor1Geo_FeatureID : GNS ou GNIS FeatureID.


Ces attributs sont répétés pour l’acteur 2 et l’action en remplaçant Actor1 par Actor2 ou Action.

### 5. La gestion des champs de données.

- DATEADDED : Ce champ permet de stocker la date à laquelle un évènement a été ajouté à la base de données maître / principale, au format YYYYMMDDHHMMSS.
- SOURCEURL :  Enregistre l’url ou la citation des premiers articles d’actualité dans lesquels apparaît cet événement.

=======================================================================
## Table MENTIONS (393,65 Go)

- GlobalEventID : L’identifiant de l’évènement mentionné dans l’article. (Il n’est pas forcément unique ici)
- EventTimeDate : Date de l’évènement au format YYYYMMDDHHMMSS, ce qui correspond au DATEADDED de l’évènement original enregistré).
- MentionTimeDate :
- MentionType :  Identifiant numérique illustrant la source du document : 1=WEB, 2= …….,  (6 en tout).
- MentionSourceName : Identifiant de la source du document
- MentionIdentifier : Identifiant externe unique du document source. (Dépend de la MentionType)
- SentenceID : La phrase dans l’article dans laquelle l’article à été mentionné. Les phrases sont numérotées de 1 à n (ombre de phrases dans l’article).
- Actor1CharOffset : Localisation dans l’article où l’acteur 1 est cité. (En terme de caractère anglais)
- Actor2CharOffset :  Localisation dans l’article où l’acteur 2 est cité. (En terme de caractère anglais)
- ActionCharOffset :  Localisation dans l’article où la description de l’action est citée. (En terme de caractère anglais)
- InRawText : Entier qui vaut 1 si l’évènement est trouvé dans un article orignal non modifié ou s’il est trouvé dans un article qui requiert une synthétisation puis une ré-écriture pour identifié l’évènement.
- Confidence : Pourcentage de confiance concernant l’extraction de l’évènement en provenance de tel article.
- MentionDocLen : Longueur du document source (en terme de caractère anglais).
- MentionDocTone : Contient les même informations que AvgTone mais uniquement pour cet article particulier.
- MentionDocTranslationInfo : Enregistre la provenance pour les documents traduits par une machine. Ce champ est blanc pour les documents qui sont initialement en Anglais.
  - SRCLC : Code du langage source : exemple ‘fra’ pour un article qui à été traduit du français à l’anglais.
  - ENG : Citation indiquant la machine et le modèle utilisé pour traduire le texte.
- Extras : Généralement vide, cet attribut est réservé pour une future utilisation.
=======================================================================

## Table GKG (13,7To), ID de la table: gdelt-bq:gdeltv2.gkg 


GKGRECORDID :Il est unique pour chaque GKG enregistré et prend la forme “YYYYMMDDHHMMSS-X” oou “YYYYMMDDHHMMSS-TX”.

-Date: Elle est sous le format  YYYYMMDDHHMMSS,  c'est la date de publication du fichier GKG.

-SourceCollectionIdentifier:C'est un identifiant numérique qui fait référence à la source de collecte du document et permet d'identifier cette dernière. Exemple 1= Web, 2= CITATIONONLY, ...., 5 = JSTOR,...

-SourceCommonName:Il s'agit d'un autre identifiant de la soource sous format texte. Exemple JSTOR pour "JSTOR"...

-DocumentIdentifier. Il est sous format texte et est l'unique identifiant externe de la source.

-V1Counts: Il s'agit de blocs séparés par des points-virgules qui contiennent la liste des comptes trouvés dans le document. Il a la forme Count type #Count###Object Type###Location Type##Location FullName##Location CountryCode##Location ADM1Code###Location Latitude#Location Longitude#Location FeatureID

-V2Counts: Identique à l'attribut précédent sauf qu'à la fin, il ajoute un champs supplémentaie

-V1Themes: Contient la liste de tous les thèmes trouvés dans le document

-V2Themes: Il s'agit de la liste de tous les thèmes de GKG.

-V1Locations: Ce sont les emplacement trouvés dans le texte et extrait de l'algo de Leetaru 2012.

-V2Locations: Identique à l'attribut précédent à la seule différence qu'un champs supplémentaire “Location ADM2Code” y est ajouté.

-V1Persons: Liste de toutes les personnes dont le nom figure dans le texte extraits de l'algorithme de Leetaru 2012.

-V2Persons: Contient la liste de tous les noms de personnes référées dans le document, ainsi que le décalage de caractère de l'endroit approximatif où ils ont été trouvés dans le document.

-Organizations: Liste de tous les noms d'entreprises et d'organisations trouvés dans le texte extraits par le même algorithme que précédemment.

-V2Organizations: contient une liste de toutes les organisations/sociétés référencées dans le document, ainsi que les décalages de caractères d'approximativement où dans le document ils ont été trouvés.

-V2Tone: contient une liste délimitée par des virgules de six dimensions émotionnelles fondamentales.

-Dates: contient une liste de toutes les références de date dans le document, ainsi que les décalages de caractères de l'endroit approximatif où elles ont été trouvées.

-GCAM: (blocs délimités par des virgules, avec des paires clé/valeur délimitées par deux-points) Le système GCAM (Global Content Analysis Measures) exécute un ensemble de systèmes d'analyse de contenu sur chaque document et compile leurs résultats dans ce champ.

-SharingImage: Il représente la sélection par le média de l'image unique qui capture le mieux l'orientation générale et le contenu de l'histoire.

-RelatedImages: Il présente des images relatives qui complètent l'attribut précédent

-SocialImageEmbeds: Images basées sur les médias sociaux

-SocialVideoEmbeds: Vidéos basées sur les médias sociaux

-Quotations: Listes d'URL de vidéos en lignes illustrant des réactions en temps réel

-AllNames: Liste des caractéristiques des déclarations séparées par le symbole #. Exemple offset#lenght#verb#quote

-Amounts: Ce champ contient une liste de tous les noms propres référencés dans le document, ainsi que les décalages de caractères de l'endroit approximatif où ils ont été trouvés

-TranslationInfo	: Ce champ contient une liste de tous les montants numériques précis référencés dans le document, ainsi que les décalages de caractères de l'endroit approximatif où ils ont été trouvés dans le document.

-Extras: Ce champ est réservé pour contenir des données spéciales non standard applicables à des sous-ensembles spéciaux de la collection GDELT.

====================================================================================


# Objectif du projet

L’objectif de ce projet est de proposer un système de stockage distribué, résilient et performant pour répondre aux question suivantes:

### 1. Afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).

Pour cette question, nous avons besoin :

- (Jour de l’évènement exact : DATEADDED, table EVENT) ?
- Jour de l’évènement : Day, table EVENT
- Pays de l’évènement : ActionGeo_CountryCode, table EVENT
- Langue de l’évènement : MentionDocTranslationInfo, Table MENTIONS
- Nombre d’évènements : GlobalEventID, table EVENT et MENTIONS (dont on fera la somme pour obtenir le nombre d’évènements)
- ( Nombre d’articles : NumArticles, table EVENT ) ?

*Questions* :
- On cherche ici le nombre total d’articles par évènements ? Ou le nombre total d’évènements ?
- Que signifie le « / » dans nombre d’articles/évènements


### 2. Pour un pays donné en paramètre, affichez les évènements qui y ont eu place triées par le nombre de mentions (tri décroissant); permettez une agrégation par jour/mois/année.

Pour cette question, nous avons besoin :

- Pays des évènements : ActionGeo_CountryCode, table EVENT
- Les évènements : GlobalEventID, table EVENT
- Notre de mentions : NumMentions, table EVENT

Pour permettre l’agrégation par jour/mois/année, il faut également :

- ( DATEADDED, table EVENT ) ?
- Jour : Day, table EVENT
- Mois : MonthYear, table EVENT
- Année : Year, table EVENT


### 3. Pour une source de donnés passée en paramètre (gkg.SourceCommonName) affichez les thèmes, personnes, lieux dont les articles de cette sources parlent ainsi que le nombre d’articles et le ton moyen des articles (pour chaque thème/personne/lieu); permettez une agrégation par jour/mois/année.

Pour cette question, nous avons besoin :

- SourceCommonName, table GKG
- Thèmes :
- Personnes :
- Lieux :
- Nombre d’articles :
- Ton moyen des articles (pour chaque thème/personne/lieu) :

Pour permettre l’agrégation par jour/mois/année, il faut également :

- ( DATEADDED, table EVENT ) ?
- Jour : Day, table EVENT
- Mois : MonthYear, table EVENT
- Année : Year, table EVENT


### 4. Etudiez l’évolution des relations entre deux pays (specifies en paramètre) au cours de l’année. Vous pouvez vous baser sur la langue de l’article, le ton moyen des articles, les themes plus souvent citées, les personnalités ou tout element qui vous semble pertinent.
