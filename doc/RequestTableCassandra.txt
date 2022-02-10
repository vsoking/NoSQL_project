%cassandra

USE gdelt;

DROP TABLE IF EXISTS requete_1;
DROP TABLE IF EXISTS requete_2;

DROP TABLE IF EXISTS requete_3_1;
DROP TABLE IF EXISTS requete_3_2;
DROP TABLE IF EXISTS requete_3_3;

DROP TABLE IF EXISTS requete_4;

CREATE TABLE requete_1
(
    Day int,
    ActionGeo_CountryCode text,
    MentionDocTranslationInfo text,
    NbTotalMentions int,
    NbEvents int,
    PRIMARY KEY (Day, ActionGeo_CountryCode, MentionDocTranslationInfo)
);

CREATE TABLE requete_2
(
    GLOBALEVENTID int,
    ActionGeo_CountryCode text,
    NumMentions int,
    day text,
    month text,
    year text,
    PRIMARY KEY (GLOBALEVENTID, ActionGeo_CountryCode, day, month,year)
);

# request 3

CREATE TABLE requete_3_1 (
    SourceCommonName text,
    day text,
    month text,
    year text,
    theme  text,
    avg_tone float,
    NbArticles int,
    PRIMARY KEY (SourceCommonName, day, month, year, theme)
);

CREATE TABLE requete_3_2
(
    SourceCommonName  text,
    day text,
    month text,
    year text,
    person  text,
    avg_tone float,
    NbArticles int,
    PRIMARY KEY (SourceCommonName, day, month, year, person)
);

CREATE TABLE requete_3_3 (
    SourceCommonName  text,
    day text,
    month text,
    year text,
    Locations  text,
    avg_v2tone float,
    NbArticles int,
    PRIMARY KEY (SourceCommonName, day, month, year, Locations)
);

# request 4

CREATE TABLE requete_4 (
    Country1 text,
    Country2 text,
    jour text,
    mois text,
    annee text,
    nombre_article text,
    Tone_mean float,
    PRIMARY KEY (Country1, Country2, jour, mois, annee)
);
