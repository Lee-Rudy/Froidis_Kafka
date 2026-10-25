# Dossier d'architecture : plateforme d'événements FROIDIS

TP fil rouge MCCI (BC05, chapitre 2) : « Déployer un cluster Kafka de A à Z : la chaîne du froid ». Réalisé le 22/10/2026 sur Kafka 4.3.1 (`apache/kafka:4.3.1`), Docker 29.1.3.

## Résumé

1. FROIDIS analyse aujourd'hui ses mesures de température le lendemain (traitement par lots). L'objectif est de réagir pendant une dérive.
2. Besoin : 500 capteurs, 500 msg/s en moyenne, 1 000 en pointe, 200 o/message, ordre par capteur, 7 jours d'historique brut et 30 jours d'alertes.
3. Dimensionnement : 0,1 Mo/s en moyenne, 60,48 Go bruts sur 7 jours, 181,44 Go répliqués ×3, soit ≈ 236 Go avec 30 % de marge.
4. Architecture : KRaft à rôles séparés, 3 contrôleurs votants (1, 2, 3 ; port 9093) et 3 brokers (4, 5, 6 ; port 19092) dans 3 zones (`broker.rack` A, B, C).
5. Configuration écrite à la main (`.properties`), identifiants dans `.env`, formatage explicite, volumes nommés.
6. Quatre topics : `mesures-brutes` (6 partitions), `mesures-agregees`, `alertes-temperature`, `etat-capteurs` (compacté), tous en RF 3 et `min.insync.replicas=2`.
7. Durabilité : `acks=all` et idempotence. Garantie : aucune perte de message confirmé si **un** broker tombe.
8. Démontré sur le cluster réel : ordre par clé, groupes indépendants et rejeu, sortie et retour dans l'ISR, refus d'écriture sous le min ISR, compaction, réaffectation et leaders préférés.
9. Exploitation : tableau de supervision, maintenance progressive, reprise disque, sécurité de production (TLS/SASL/ACL).
10. Recette : **8 critères sur 8 OK**. Les alertes sont **simulées** en attendant le job Flink du chapitre 3.

## Ordre de lecture

| # | Fichier | Contenu | Phases |
|---|---|---|---|
| 1 | [01-contexte-et-hypotheses.md](01-contexte-et-hypotheses.md) | Contexte, fiche de besoin, pannes couvertes, temps réel ou lots | A.1 |
| 2 | [02-dimensionnement.md](02-dimensionnement.md) | Tableau de calculs, écarts avec le poste, questions A | A.2, A.3 |
| 3 | [03-architecture.md](03-architecture.md) | Schéma annoté, justifications, passage en production | B |
| 4 | [04-configuration-kafka.md](04-configuration-kafka.md) | `.env`, `.properties`, Compose, formatage, quorum, topics, questions C et D | C, D |
| 5 | [05-verifications-et-preuves.md](05-verifications-et-preuves.md) | Production, groupes, pannes, segments, compaction, réaffectation, questions E à I | E à I |
| 6 | [06-exploitation-securite.md](06-exploitation-securite.md) | Supervision, maintenance, sécurité, reprise disque, sauvegardes, archivage | J |
| 7 | [07-kafka-flink-et-limites.md](07-kafka-flink-et-limites.md) | Simulation des sorties, Kafka face à Flink, avantages, limites, risques | K, livrable |
| 8 | [08-recette.md](08-recette.md) | Les 8 critères de recette, statut et preuves | L |
| 9 | [09-questions-oral.md](09-questions-oral.md) | Réponses aux 10 questions de soutenance | oral |
| 10 | [10-criteres-7-8-guide-H-I-J.md](10-criteres-7-8-guide-H-I-J.md) | Critères 7 et 8 : guide pas à pas des phases H, I et J, rétention démontrée, tableau de bord exécutable, maintenance, sécurité, remise en service (PDF : `pdf/`) | H, I, J, L |

## Annexes (projet technique)

- [`../README.md`](../README.md) : démarrage, arrêt, rôle des fichiers, identifiants.
- [`../Command.md`](../Command.md) : journal commenté de toutes les commandes, problèmes rencontrés.
- [`../preuves/`](../preuves/) : sorties réelles, un fichier par étape.
- [`../json/`](../json/) : fichiers de la phase I.
- [`../scripts/tableau-de-bord.sh`](../scripts/tableau-de-bord.sh) : tableau de bord de supervision en ligne de commande (phase J, lecture seule).
- [`../config/`](../config/), [`../docker-compose-froid.yml`](../docker-compose-froid.yml), [`../.env`](../.env).
