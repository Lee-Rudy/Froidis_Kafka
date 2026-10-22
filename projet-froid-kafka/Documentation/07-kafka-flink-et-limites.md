# 7. Kafka, Flink, avantages et limites (phase K)

## 7.1 Sorties simulées (phase K)

> **Il s'agit d'une SIMULATION.** Aucun job Flink ne tourne : les messages ci-dessous ont été **tapés à la main** avec le producteur console (`acks=all`, idempotence), pour montrer le circuit des topics de sortie. Ce n'est pas une détection automatique.

Extrait de `preuves/K-simulation-sorties.txt` :
```
mesures-agregees     Partition:1 Offset:0 CAP-001 {"sensorId":"CAP-001","fenetreDebut":"2026-10-21T09:00:00Z","fenetreFin":"2026-10-21T09:05:00Z","nbMesures":4,"tempMin":4.2,"tempMax":9.0,"tempMoy":7.0,"source":"SIMULATION"}
mesures-agregees     Partition:0 Offset:0 CAP-002 {..."source":"SIMULATION"}
mesures-agregees     Partition:2 Offset:0 CAP-003 {..."source":"SIMULATION"}
alertes-temperature  Partition:1 Offset:0 CAP-001 {"alertId":"A001","sensorId":"CAP-001","regle":"T>8 pendant 2 min","source":"SIMULATION"}
```
Les valeurs de l'agrégat sont **illustratives**. Un vrai calcul sur `mesures-brutes` aurait trouvé, pour CAP-001 entre 09:00 et 09:05, 5 mesures (E001, E003, E004, E006, E007) avec un maximum de 9,4 °C. L'écart montre ce que Flink apportera : un calcul exact et reproductible à la place d'une saisie. La clé `sensorId` est conservée : les sorties de CAP-001 sont, elles aussi, dans une seule partition.

## 7.2 Ce qui relève de Kafka et ce qui relève de Flink (K-1)

| Kafka : **transport fiable** des mesures | Flink : **interprétation métier** (chapitre 3) |
|---|---|
| Recevoir les mesures des capteurs et les **acquitter** de façon durable (RF 3, min ISR 2, `acks=all`) | Lire `mesures-brutes` en **temps d'événement** (champ `ts`), avec des watermarks |
| Garantir l'**ordre par capteur** (clé → partition) | **Écarter les valeurs invalides** (JSON mal formé, température aberrante, capteur inconnu) |
| **Conserver** 7 jours et permettre le **rejeu** (offsets, groupes) | **Agréger** par capteur et par fenêtre (min, max, moyenne) → `mesures-agregees` |
| **Diffuser** à plusieurs applications indépendantes (groupes) | Évaluer la règle « T > 8 °C **pendant 2 minutes** » : définir la sémantique de la fenêtre (glissante ou de session), l'état par capteur, le **traitement des retards** (latence admise, sortie latérale) → `alertes-temperature` |
| Garder le **dernier état** par capteur (compaction) | Dédupliquer par `eventId` (doublons métier, voir E-Q3) |
| Survivre à la panne d'un broker, réaffecter, superviser | Checkpoints et exactly-once vers Kafka (sink transactionnel), reprise sur un offset |

Kafka ne « comprend » pas les mesures : il ne sait pas ce qu'est une température ni une durée de 2 minutes. Flink ne stocke pas durablement le flux : il s'appuie sur Kafka pour la source, le rejeu et les sorties.

## 7.3 Avantages et limites

### Kafka face au traitement par lots

| Avantages | Limites |
|---|---|
| Latence de quelques millisecondes au lieu de J+1 : réaction pendant la dérive | Plus d'infrastructure à exploiter (6 nœuds, supervision, sécurité) qu'un partage de fichiers |
| Un seul flux pour plusieurs usages (temps réel, analyse, rejeu) | Les compétences d'exploitation sont plus rares |
| Rejeu natif d'un incident (relecture par offset, nouveau groupe) | Rétention limitée (7 j) : il faut un archivage séparé pour le long terme |
| Tolérance aux pannes démontrée (G) | L'ordre n'est garanti que par partition, pas globalement |
| Le lot reste possible : l'analyse différée lit le même topic | Les requêtes ad hoc et les jointures historiques sont plus simples dans un entrepôt de données |

### Kafka face à Flink

Ce ne sont pas des concurrents, mais deux couches complémentaires. Kafka est le **journal durable et partagé** (stockage, diffusion, rejeu). Flink est le **moteur de calcul avec état** (fenêtres, temps d'événement, alertes). Kafka seul (consommateurs simples, Kafka Streams) suffirait pour des transformations simples, mais la règle « 2 minutes au-dessus de 8 °C » avec des données en retard relève d'un moteur de flux complet.

### Risques restants

| Risque | Conséquence | Mitigation |
|---|---|---|
| Laboratoire sur **un seul hôte** | Les « zones » ne sont pas indépendantes : une panne du PC arrête tout | 3 hôtes / 3 zones en production (`03`) |
| PLAINTEXT, sans authentification ni ACL | Lecture et écriture par n'importe qui sur le réseau | TLS + SASL + ACL (`06`) |
| Deux brokers perdus | Écritures refusées : les capteurs doivent tamponner localement | Tampon des passerelles, `delivery.timeout.ms` adapté, alertes `UnderMinIsr` |
| Doublons métier | Mesures ou alertes comptées deux fois | `eventId` et déduplication dans Flink |
| Pas de sauvegarde ni d'archivage | Perte définitive après 7 j ou après une erreur humaine | Export vers un stockage objet, MirrorMaker 2 |
| Heap de 512 Mo, disque de laboratoire | Ne tient pas la charge réelle de 7 jours (≈ 236 Go) | Dimensionnement de `02` |
| Données en retard ou horloges des capteurs | Alerte fausse ou manquée | Temps d'événement, watermarks, latence admise (Flink) |
| Agrégats et alertes encore **simulés** | Aucune détection réelle aujourd'hui | Job Flink au chapitre 3 |
