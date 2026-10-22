# 1. Contexte et hypothèses (phase A.1)

## 1.1 Contexte

FROIDIS exploite un réseau de chambres froides industrielles. Aujourd'hui, les mesures de température sont déposées dans un fichier partagé, puis étudiées **le lendemain**. Ce traitement par lots ne permet pas de réagir quand une anomalie apparaît : une chambre qui dérive à 9 °C pendant la nuit n'est vue qu'au matin, après la perte éventuelle de la marchandise.

Le projet met en place un cluster Kafka qui sert de **colonne vertébrale événementielle**. Il reçoit et conserve les mesures, les diffuse à plusieurs services et survit à la panne d'un broker. Le calcul des alertes est confié à Flink (chapitre 3).

> Les chiffres ci-dessous sont les **hypothèses de travail du sujet**, pas des mesures d'un exploitant réel. Le seuil de 8 °C est pédagogique, pas une prescription sanitaire.

## 1.2 Fiche de besoin

| Rubrique | Besoin | Traduction Kafka |
|---|---|---|
| **Producteurs** | 500 capteurs de température (passerelles IoT), 1 mesure/s/capteur. Plus tard : job Flink (agrégats, alertes). | Producteurs avec clé = `sensorId`, `acks=all`, `enable.idempotence=true` |
| **Consommateurs** | Tableau de bord temps réel, service d'alertes, analyse différée (qualité, incidents), job Flink | Un **groupe de consommateurs par application** : chacune lit tout le flux, à son rythme |
| **Débit moyen** | 500 msg/s × 200 o = 100 000 o/s (0,1 Mo/s) | Voir `02-dimensionnement.md` |
| **Débit de pointe** | 2 × moyen = 1 000 msg/s, 0,2 Mo/s | Base du dimensionnement réseau/CPU |
| **Rétention** | Mesures brutes : 7 jours (rejouer). Alertes et agrégats : 30 jours. Dernier état : dernière valeur par capteur | `retention.ms=604800000`, `retention.ms=2592000000`, `cleanup.policy=compact` |
| **Pertes acceptables** | Aucune perte d'un message **confirmé** (acquitté), dans la limite des pannes prévues : **perte d'un broker sur trois** | RF=3, `min.insync.replicas=2`, `acks=all`, `unclean.leader.election.enable=false` |
| **Doublons acceptables** | Pas de doublon technique dû aux tentatives du producteur. Les doublons **métier** (même événement envoyé deux fois par l'application) restent possibles : chaque mesure porte un `eventId` pour les filtrer en aval | Idempotence du producteur (`producerId` + `sequence`) |
| **Ordre** | Ordonné **par capteur**, pas globalement | Clé = `sensorId` : toutes les mesures d'un capteur vont dans la même partition |
| **Anomalie métier (exemple)** | T > 8 °C pendant 2 minutes | Calculée par Flink (fenêtres en temps d'événement), pas par Kafka |
| **Zones** | 3 zones indépendantes, un broker par zone | `broker.rack=A/B/C` : une réplique par zone |

## 1.3 Pannes couvertes (et non couvertes)

| Couvert | Non couvert |
|---|---|
| Arrêt d'un broker (ou d'une zone) : lectures et écritures continuent | Perte simultanée de deux brokers : les écritures `acks=all` sont **refusées** (pas de perte, mais indisponibilité en écriture) |
| Arrêt d'un contrôleur sur trois : le quorum garde sa majorité | Perte de deux contrôleurs : plus de majorité, plus de changement de métadonnées |
| Retransmission par le producteur (réseau instable) : pas de doublon grâce à l'idempotence | Perte simultanée des trois disques, erreur humaine (suppression de topic), corruption logicielle : seule une **sauvegarde** protège |

On ne promet donc pas « zéro perte en toutes circonstances » : la garantie vaut pour **un broker perdu sur trois**.

## 1.4 Pourquoi le temps réel plutôt que le lot

| Traitement par lots (actuel) | Flux temps réel (cible) |
|---|---|
| Fichier analysé le lendemain : latence de plusieurs heures | Mesure disponible en quelques millisecondes pour tous les consommateurs |
| Une dérive est découverte après coup | L'alerte « > 8 °C pendant 2 min » peut partir 2 minutes après le début de la dérive |
| Un seul consommateur du fichier, couplage fort | Plusieurs applications indépendantes lisent le même flux (groupes distincts) |
| Rejouer un incident = retrouver le bon fichier | Rejouer = relire le topic depuis un offset (7 jours d'historique) |

Le lot reste pertinent pour l'analyse différée (rapports, statistiques) : elle peut lire le même topic, avec son propre groupe, sans gêner le temps réel.
