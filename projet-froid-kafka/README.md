# Projet FROIDIS : cluster Kafka de la chaîne du froid

TP fil rouge MCCI (BC05, chapitre 2). Cluster **Kafka 4.3.1** en **KRaft à rôles séparés** : 3 contrôleurs et 3 brokers répartis sur 3 zones simulées, configuration écrite à la main, formatage explicite.

> **Laboratoire, pas production.** Les listeners sont en PLAINTEXT, sans authentification, sur un seul hôte, avec une heap de 512 Mo par nœud. Ne pas exposer ce cluster hors d'un réseau isolé. Voir `Documentation/03` et `06` pour la cible de production.

## Prérequis

- Docker Desktop (ou Docker Engine) avec Compose v2. Testé avec Docker 29.1.3 et Compose v2.40.3.
- Image `apache/kafka:4.3.1` (téléchargée automatiquement sinon).
- Environ 4 Go de mémoire libre pour Docker (6 × 512 Mo de heap, plus la JVM).
- Sous Git Bash (Windows) : `export MSYS_NO_PATHCONV=1` avant toute commande `docker exec … /opt/kafka/...`.

## Démarrage

Depuis le dossier `projet-froid-kafka/` :

```bash
docker compose -f docker-compose-froid.yml config --quiet      # valide le fichier et la lecture de .env
docker compose -f docker-compose-froid.yml up -d --wait        # démarre et attend que les 6 services soient healthy
docker compose -f docker-compose-froid.yml ps --format "{{.Name}} · {{.Status}}"
```

Le premier démarrage formate les volumes (17 s sur notre poste, jusqu'à ≈ 4 min sur un poste plus lent). Vérifier ensuite le quorum :

```bash
docker exec froid-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --replication
```

Pour travailler dans le conteneur : `docker exec -it froid-kafka-1 bash`, puis `cd /opt/kafka`. Adresse des outils : `kafka-1:19092`.

## Arrêt et relance

```bash
docker compose -f docker-compose-froid.yml stop    # arrête les conteneurs, garde les données
docker compose -f docker-compose-froid.yml start   # relance sans reformater
```

Au redémarrage, chaque nœud affiche `All of the log directories are already formatted.`

> ⚠️ **Ne jamais lancer `docker compose -f docker-compose-froid.yml down -v`** : `-v` supprime les volumes nommés, donc toutes les données et l'identité du cluster. `down` sans `-v` supprime les conteneurs mais garde les volumes. À n'utiliser qu'avec l'accord de l'enseignant.

## Rôle des fichiers

| Fichier / dossier | Rôle |
|---|---|
| `.env` | `CLUSTER_ID` et UUID de répertoire des 3 contrôleurs, lus par Compose. **Ne plus modifier** : ils sont écrits dans les volumes. |
| `docker-compose-froid.yml` | Les 6 services (projet `projet-froid`) : formatage (`kafka-storage.sh format --ignore-formatted`) puis démarrage, healthchecks, volumes nommés |
| `config/controller-{1,2,3}.properties` | Configuration des contrôleurs (rôle, quorum, défauts des topics : 6 partitions, RF 3, min ISR 2) |
| `config/broker-{4,5,6}.properties` | Configuration des brokers (listeners, `broker.rack`, réplication des topics internes, `unclean.leader.election.enable=false`) |
| `json/` | Fichiers de la phase I : topics à déplacer, plan de réaffectation, retour arrière, élection préférée |
| `preuves/` | Sorties réelles des commandes, un fichier par étape (ex. `C6-quorum.txt`, `G3-deux-brokers-arretes.txt`) |
| `Command.md` | Journal de toutes les commandes lancées, commentées, avec les problèmes rencontrés |
| `Documentation/` | Dossier d'architecture (lire `00-index.md` en premier) |
| `Documentation/pdf/` | PDF du document 10 (critères 7 et 8) et son script de génération `generer-pdf.sh` (pandoc + Chrome) |
| `scripts/tableau-de-bord.sh` | Tableau de bord de supervision (phase J), en lecture seule : quorum, brokers, sous-réplication, min ISR, partitions hors ligne, leaders préférés, LAG, disques, journaux. `bash scripts/tableau-de-bord.sh` depuis l'hôte |

## Identifiants du cluster

| Élément | Valeur |
|---|---|
| `CLUSTER_ID` | `VJxGZOKwQIay5zzcsB_t3w` |
| UUID contrôleur 1 / 2 / 3 | `gOtFIhrIS1CROaiIe0wMyQ` / `qcQkn36mTYGryiSUju5IyA` / `KEjSrcxnTUSwHfG3A0pong` |
| Version de métadonnées | `4.3-IV0` |

## Les six nœuds

| Conteneur | Nom réseau | node.id | Rôle | Zone (`broker.rack`) | Port | Fichier |
|---|---|---|---|---|---|---|
| froid-controller-1 | controller-1 | 1 | contrôleur | — | 9093 | `config/controller-1.properties` |
| froid-controller-2 | controller-2 | 2 | contrôleur | — | 9093 | `config/controller-2.properties` |
| froid-controller-3 | controller-3 | 3 | contrôleur | — | 9093 | `config/controller-3.properties` |
| froid-kafka-1 | kafka-1 | 4 | broker | A | 19092 | `config/broker-4.properties` |
| froid-kafka-2 | kafka-2 | 5 | broker | B | 19092 | `config/broker-5.properties` |
| froid-kafka-3 | kafka-3 | 6 | broker | C | 19092 | `config/broker-6.properties` |

Aucun port n'est publié sur l'hôte : les clients s'exécutent dans le réseau Docker `projet-froid_default` (via `docker exec`).

## Topics métier

| Topic | Partitions | RF | min ISR | Politique |
|---|---|---|---|---|
| `mesures-brutes` | 6 | 3 | 2 | delete, 7 jours |
| `mesures-agregees` | 3 | 3 | 2 | delete, 30 jours |
| `alertes-temperature` | 3 | 3 | 2 | delete, 30 jours |
| `etat-capteurs` | 3 | 3 | 2 | compact |
