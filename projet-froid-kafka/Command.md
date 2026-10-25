# Journal des commandes : TP FROIDIS

Toutes les commandes réellement lancées le 22/10/2026, dans l'ordre, regroupées par phase.

- **Hôte** : terminal Git Bash ouvert dans `projet-froid-kafka/`. Faire d'abord `export MSYS_NO_PATHCONV=1`, sinon Git Bash convertit `/opt/kafka/...` en chemin Windows.
- **Kafka** : outils lancés dans le conteneur `froid-kafka-1`. Sans terminal interactif, ils sont lancés depuis l'hôte par `docker exec froid-kafka-1 /opt/kafka/bin/<outil>.sh ...`, ce qui équivaut à `docker exec -it froid-kafka-1 bash` puis `cd /opt/kafka` et `bin/<outil>.sh ...`.
- Producteurs : lignes envoyées par un pipe (`printf ... | docker exec -i ...`). Consommateurs : arrêtés par `--max-messages` et/ou `--timeout-ms` au lieu de Ctrl-C.
- Les sorties sont rangées dans `preuves/` (les commandes y sont redirigées avec `| tee preuves/<fichier>.txt`).

---

## Phase 0 - État des lieux (avant toute action)

### Vérifier l'existant Docker
```
docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'   # [hôte] liste les conteneurs : ancien cluster kafka-1..3 et setupkafka-controller-1..3 présents, tous arrêtés (Exited), non modifiés
docker volume ls                                                      # [hôte] liste les volumes existants (aucun volume projet-froid_*)
docker info --format 'CPUs={{.NCPU}} Mem={{.MemTotal}}'               # [hôte] ressources de Docker : 12 CPU, 8,16 Go, suffisant pour 6 nœuds à 512 Mo de heap
df -h /c ; df -h /d                                                   # [hôte] espace disque : 20 Go libres sur C: (stockage WSL2 de Docker), utilisé dans le dimensionnement
docker system df                                                      # [hôte] occupation disque de Docker
```

### Lire l'énoncé
```
pdftotext -layout -enc UTF-8 "Documetation/sujet tp/MCCI_..._V2.pdf" sujet.txt   # [hôte] extrait le texte du PDF du sujet pour le lire en entier
```

## Phases A et B - Cadrage, dimensionnement, architecture

Pas de commande : rédaction de `Documentation/01-contexte-et-hypotheses.md`, `02-dimensionnement.md` et `03-architecture.md`.

## Phase C - Construire les six nœuds

### C1 - Préparer le dossier du projet
```
mkdir -p projet-froid-kafka/{config,preuves,json,Documentation}   # [hôte] crée l'arborescence du projet
```

### C2 - Vérifier Docker et l'image (preuves/C2-versions.txt)
```
docker --version                                            # [hôte] version du moteur : 29.1.3
docker compose version                                      # [hôte] version de Compose : v2.40.3-desktop.1
docker pull apache/kafka:4.3.1                              # [hôte] récupère l'image (déjà présente : « Image is up to date »)
docker image inspect apache/kafka:4.3.1 --format '{{.Id}} {{.Created}}'   # [hôte] note l'identifiant exact de l'image
```

### C3 - Générer les identifiants (preuves/C3-uuid.txt)
```
docker run --rm apache/kafka:4.3.1 /opt/kafka/bin/kafka-storage.sh random-uuid   # [hôte] lancée 4 fois : CLUSTER_ID puis UUID_C1, UUID_C2, UUID_C3, rangés dans .env
```

### C4 / C5 - Fichiers de configuration et Compose
Écrits à la main : `config/controller-1..3.properties`, `config/broker-4..6.properties`, `docker-compose-froid.yml`, `.env`.

### C6 - Démarrer le cluster et vérifier le quorum (preuves/C6-*.txt)
```
docker compose -f docker-compose-froid.yml config --quiet          # [hôte] valide la syntaxe du Compose et la substitution des variables de .env
docker compose -f docker-compose-froid.yml config | grep -nE 'QUORUM_INITIAL|CLUSTER_ID'   # [hôte] vérifie que ${CLUSTER_ID} et les UUID sont bien remplacés
docker compose -f docker-compose-froid.yml up -d --wait            # [hôte] crée réseau, volumes et 6 conteneurs, attend l'état healthy (17 s ici)
docker compose -f docker-compose-froid.yml ps --format "{{.Name}} · {{.Status}}"   # [hôte] six services « (healthy) »
docker logs froid-controller-1 --tail 30                           # [hôte] fin du journal d'un contrôleur
docker logs froid-kafka-1 --tail 30                                # [hôte] fin du journal d'un broker
docker logs <conteneur> 2>&1 | grep -E "Formatting|already formatted"   # [hôte] vérifie le formatage de chacun des 6 nœuds
docker logs <conteneur> 2>&1 | grep -cE " ERROR "                  # [hôte] compte les lignes ERROR (0 sur chacun des 6 nœuds)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status        # [Kafka] leader du quorum, votants (avec UUID) et observateurs
docker exec froid-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --replication   # [Kafka] LogEndOffset et Lag de chaque nœud
```

## Phase D - Créer les topics métier (preuves/D-topics.txt, preuves/D-configs.txt)

```
bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic mesures-brutes --partitions 6 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms=604800000         # [Kafka] mesures brutes, 7 jours
bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic mesures-agregees --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms=2592000000     # [Kafka] sorties Flink, 30 jours
bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic alertes-temperature --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms=2592000000  # [Kafka] alertes, 30 jours
bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic etat-capteurs --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact                                        # [Kafka] dernier état par capteur, compacté
bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list                                          # [Kafka] liste les 4 topics créés
bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic <topic>                      # [Kafka] leaders, répliques, ISR (lancée pour chacun des 4 topics)
bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --entity-type topics --entity-name mesures-brutes   # [Kafka] configuration du topic et origine de chaque valeur (synonyms)
bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --entity-type brokers --entity-default             # [Kafka] défaut dynamique du cluster : min.insync.replicas=2, issu du fichier des contrôleurs
bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --entity-type brokers --entity-name 4              # [Kafka] aucune configuration dynamique propre au broker 4
```
(Toutes ces commandes sont lancées depuis l'hôte, préfixées par `docker exec froid-kafka-1 /opt/kafka/`.)

## Phase E - Envoyer des mesures de test (preuves/E1-production.txt, E2-lecture.txt, E3-ordre-par-cle.txt)

```
cat <<'EOF' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --reader-property parse.key=true --reader-property key.separator=: --command-property acks=all --command-property enable.idempotence=true
CAP-001:{"eventId":"E001",...,"temperatureC":4.2}
...
CAP-003:{"eventId":"E005",...,"temperatureC":3.8}
EOF
#   [hôte -> Kafka] envoie les 5 mesures du sujet avec clé (texte avant « : »), acks=all et idempotence ; -i transmet le pipe au conteneur
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true --max-messages 5 --timeout-ms 20000
#   [Kafka] relit les 5 mesures avec partition et offset (les mesures sont sorties dès le 1er essai, voir Problèmes)
cat <<'EOF' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh ... (mêmes options)
CAP-001:{"eventId":"E006",...}   CAP-001:{"eventId":"E007",...}
EOF
#   [hôte -> Kafka] question E-1 : 2 mesures de plus pour CAP-001
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --partition 1 --offset earliest --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true --max-messages 5 --timeout-ms 15000
#   [Kafka] lit la seule partition 1 : CAP-001 aux offsets 0 à 4, dans l'ordre d'envoi
docker exec froid-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes
#   [Kafka] offset de fin de chaque partition : les offsets sont propres à chaque partition
```

## Phase F - Consommateurs, groupes et relecture (preuves/F1-*.txt, F2-*.txt, F3-*.txt)

```
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --group supervision-froid --from-beginning --max-messages 7 --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true --timeout-ms 20000
#   [Kafka] groupe supervision-froid : lit les 7 mesures puis s'arrête en validant ses offsets (remplace Ctrl-C)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh ... --group analyse-qualite --from-beginning --max-messages 7 ...
#   [Kafka] groupe analyse-qualite : reçoit lui aussi les 7 mesures (groupes indépendants)
cat <<'EOF' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh ... (options de la phase E)
CAP-001:{"eventId":"E008",...}  CAP-003:{"eventId":"E009",...}  CAP-002:{"eventId":"E010",...}
EOF
#   [hôte -> Kafka] 3 nouvelles mesures pendant que analyse-qualite est arrêté
docker exec froid-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group analyse-qualite
#   [Kafka] CURRENT-OFFSET, LOG-END-OFFSET, LAG : LAG total = 3
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh ... --group analyse-qualite --max-messages 3 ...
#   [Kafka] relance SANS --from-beginning : reprend exactement les 3 nouvelles mesures
docker exec froid-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group analyse-qualite
#   [Kafka] LAG revenu à 0
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh ... --group rejeu-incident --from-beginning --max-messages 10 ...
#   [Kafka] nouveau groupe : relit les 10 mesures conservées
docker exec froid-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --list
#   [Kafka] liste les groupes (dont console-consumer-8079, groupe généré par le consommateur sans --group de la phase E)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group supervision-froid
#   [Kafka] supervision-froid garde un LAG de 3 : chaque groupe a ses propres offsets
```

## Phase G - Tolérance aux pannes (preuves/G1-*.txt à G4-*.txt)

### G1 - Situation initiale
```
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic mesures-brutes               # [Kafka] leaders 5,6,4,5,6,4 ; ISR à 3 partout
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions        # [Kafka] sortie vide
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic __consumer_offsets | head -1 # [Kafka] 50 partitions, RF 3
```

### G2 - Perte d'un broker
```
docker stop froid-kafka-3                                                                                                                 # [hôte] arrête le broker 6 (zone C)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic mesures-brutes               # [Kafka] ISR à 2 (4,5) ; leaders de 6 basculés sur 4
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions        # [Kafka] 65 partitions : 50 __consumer_offsets + 15 métier
echo 'CAP-001:{"eventId":"E011",...}' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --reader-property parse.key=true --reader-property key.separator=: --command-property acks=all --command-property enable.idempotence=true
#   [hôte -> Kafka] écriture acks=all acceptée (code retour 0)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --partitions 1    # [Kafka] fin de partition 1 : 6 -> 7, l'écriture est bien enregistrée
docker exec froid-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status                    # [Kafka] quorum intact (LeaderId 3)
```

### G3 - Perte d'un deuxième broker
```
docker stop froid-kafka-2                                                                                                                 # [hôte] arrête le broker 5 (zone B)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic mesures-brutes               # [Kafka] Isr: 4, Elr: 5 sur les 6 partitions
echo 'CAP-001:{"eventId":"E012",...}' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh ... (mêmes options)
#   [hôte -> Kafka] NOT_ENOUGH_REPLICAS x3 puis NotEnoughReplicasException (abandon en 3 s)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --partitions 1    # [Kafka] toujours 7 : rien n'a été écrit
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes --partition 1 --offset 6 --max-messages 1 --timeout-ms 15000 --formatter-property print.offset=true
#   [Kafka] la lecture reste possible (leader 4 vivant)
```

### G4 - Restaurer le cluster
```
docker start froid-kafka-2 froid-kafka-3                                                                                                  # [hôte] relance les deux brokers
while :; do n=$(docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions | grep -c Partition:); echo $n; [ "$n" = 0 ] && break; sleep 5; done
#   [hôte] boucle d'attente sur la sous-réplication : 65 -> 0 en 12 s
echo 'CAP-001:{"eventId":"E013",...}' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh ... (mêmes options)      # [hôte -> Kafka] écriture acceptée (fin de partition 1 : 8)
while :; do <describe mesures-brutes | awk : compte les partitions où Leader != 1er de Replicas>; [ 0 ] && break; sleep 20; done
#   [hôte] boucle d'attente des leaders préférés : 4 partitions menées par 4 jusqu'à T+268 s, retour à T+293 s (auto.leader.rebalance.enable, 300 s)
docker compose -f docker-compose-froid.yml ps --format "{{.Name}} · {{.Status}}"                                                       # [hôte] six services healthy
docker exec froid-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --replication               # [Kafka] Lag 0 sur les 6 nœuds
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --exclude-internal | awk ...        # [Kafka] 0 partition métier hors leader préféré : cluster restauré
```

## Phase H - Segments, rétention et compaction (preuves/H1-segments.txt, H2-*.txt)

### H1 - Le stockage sur disque
```
docker exec froid-kafka-1 bash -c 'cd /opt/kafka && bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --all --entity-type brokers --entity-name 4 | grep -E "^ *log\.(dirs|segment\.bytes|roll\.hours)="'
#   [Kafka] log.dirs=/var/lib/kafka/data, log.segment.bytes=1 Gio, log.roll.hours=168 (valeurs par défaut)
docker exec froid-kafka-1 ls /var/lib/kafka/data                                     # [Kafka] un répertoire par partition hébergée + fichiers de checkpoint et meta.properties
docker exec froid-kafka-1 ls -l /var/lib/kafka/data/mesures-brutes-1                 # [Kafka] segment 00000000000000000000 : .log, .index et .timeindex préalloués à 10 Mo
docker exec froid-kafka-1 /opt/kafka/bin/kafka-dump-log.sh --files /var/lib/kafka/data/mesures-brutes-1/00000000000000000000.log --print-data-log
#   [Kafka] lots (baseOffset, count, producerId, sequence) et enregistrements de CAP-001
```

### H2 - Le topic compacté
```
docker exec froid-kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --alter --entity-type topics --entity-name etat-capteurs --add-config segment.ms=20000,min.cleanable.dirty.ratio=0.01
#   [Kafka] réglages de DÉMONSTRATION : segment fermé après 20 s, nettoyage dès 1 % de données « sales »
printf 'CAP-001:4.2\nCAP-002:5.1\nCAP-001:8.7\nCAP-003:3.8\nCAP-001:9.0\n' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic etat-capteurs --reader-property parse.key=true --reader-property key.separator=:
#   [hôte -> Kafka] 5 valeurs d'état
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic etat-capteurs --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true --timeout-ms 10000
#   [Kafka] état AVANT : CAP-001 aux offsets 0, 1, 2 de la partition 1
sleep 25 ; printf 'CAP-001:9.2\n' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh ... --topic etat-capteurs ...
#   [hôte -> Kafka] plus de 20 s après : l'arrivée de 9.2 dans la partition 1 ferme le segment (segment.ms dépassé)
sleep 65 ; docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh ... --topic etat-capteurs --from-beginning ...
#   [Kafka] état APRÈS (1 min) : offsets 0 et 1 de CAP-001 supprimés ; 9.0 (offset 2) et 9.2 (offset 3) restent
docker exec froid-kafka-1 ls -l /var/lib/kafka/data/etat-capteurs-1                  # [Kafka] ancien segment compacté (78 o) + segment actif 00000000000000000003
docker exec froid-kafka-1 /opt/kafka/bin/kafka-dump-log.sh --files /var/lib/kafka/data/etat-capteurs-1/00000000000000000000.log --print-data-log
#   [Kafka] le segment compacté ne contient plus que l'offset 2 (« Non-consecutive offsets »)
printf 'CAP-002:NULL\n' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic etat-capteurs --reader-property parse.key=true --reader-property key.separator=: --reader-property null.marker=NULL
#   [hôte -> Kafka] tombstone (valeur nulle) pour CAP-002
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh ... --topic etat-capteurs --from-beginning ...   # [Kafka] « Partition:0 Offset:1 CAP-002 null »
docker exec froid-kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --alter --entity-type topics --entity-name etat-capteurs --delete-config segment.ms,min.cleanable.dirty.ratio
#   [Kafka] RETRAIT des réglages de démonstration
docker exec froid-kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --entity-type topics --entity-name etat-capteurs
#   [Kafka] il ne reste que cleanup.policy=compact et min.insync.replicas=2
```

## Phase I - Réaffectation et leaders préférés (preuves/I1-I2-generate.txt, I3-I4-reaffectation.txt, I5-election-preferee.txt ; json/)

```
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic tp-migration --partitions 3 --replication-factor 1
#   [Kafka] topic de démonstration, 1 seule réplique (leaders 5, 6, 4)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic tp-migration      # [Kafka] RF 1
docker exec -i froid-kafka-1 bash -c 'cat > /tmp/topics-migration.json' < json/topics-migration.json       # [hôte] copie le JSON (LF, UTF-8 sans BOM) dans le conteneur par pipe
docker exec -i froid-kafka-1 bash -c 'cat > /tmp/plan-migration.json' < json/plan-migration.json           # [hôte] idem pour le plan [4,5,6] [5,6,4] [6,4,5]
docker exec -i froid-kafka-1 bash -c 'cat > /tmp/election-migration.json' < json/election-migration.json   # [hôte] idem pour la liste d'élection
docker exec froid-kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:19092 --topics-to-move-json-file /tmp/topics-migration.json --broker-list 4,5,6 --generate
#   [Kafka] affiche l'affectation actuelle (sauvée dans json/retour-arriere-migration.json) et une proposition (non utilisée : RF 1)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:19092 --reassignment-json-file /tmp/plan-migration.json --execute
#   [Kafka] lance la réaffectation vers 3 répliques par partition
docker exec froid-kafka-1 /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:19092 --reassignment-json-file /tmp/plan-migration.json --verify
#   [Kafka] « Reassignment ... is completed » x3, puis retire les quotas (throttles)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic tp-migration      # [Kafka] RF 3, mais leaders toujours 5, 6, 4
docker exec froid-kafka-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:19092 --election-type PREFERRED --path-to-json-file /tmp/election-migration.json
#   [Kafka] élection préférée limitée aux 3 partitions de tp-migration
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic tp-migration      # [Kafka] leaders 4, 5, 6 = 1er broker de chaque liste
docker exec froid-kafka-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server kafka-1:19092 --election-type PREFERRED --path-to-json-file /tmp/election-migration.json
#   [Kafka] seconde élection : « Valid replica already elected »
docker exec froid-kafka-1 bash -c "/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --all --entity-type brokers --entity-name 4 | grep -E 'auto.leader.rebalance|leader.imbalance'"
#   [Kafka] auto.leader.rebalance.enable=true, contrôle toutes les 300 s
```

## Phase J - Exploitation, sécurité

Le 22/10 : pas de commande, rédaction de `Documentation/06-exploitation-securite.md`. Le 25/10 : `Documentation/10-criteres-7-8-guide-H-I-J.md`, `scripts/tableau-de-bord.sh`, et les procédures jouées sur un second poste (section suivante).

## Phase K - Simuler les sorties (preuves/K-simulation-sorties.txt)

```
cat <<'EOF' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic mesures-agregees --reader-property parse.key=true --reader-property key.separator=: --command-property acks=all --command-property enable.idempotence=true
CAP-001:{"sensorId":"CAP-001",...,"source":"SIMULATION"}   (3 lignes : CAP-001, CAP-002, CAP-003)
EOF
#   [hôte -> Kafka] SIMULATION à la main d'agrégats que produira Flink (valeurs illustratives, non calculées)
cat <<'EOF' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh ... --topic alertes-temperature ...
CAP-001:{"alertId":"A001","sensorId":"CAP-001","regle":"T>8 pendant 2 min","source":"SIMULATION"}
EOF
#   [hôte -> Kafka] SIMULATION d'une alerte (ligne du sujet)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic mesures-agregees --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true --max-messages 3 --timeout-ms 15000
#   [Kafka] relit les 3 agrégats simulés
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh ... --topic alertes-temperature ... --max-messages 1 ...
#   [Kafka] relit l'alerte simulée
```

## Phase L - Clôture et arrêt (preuves/L1-liste-cloture.txt, L2-stop-start.txt, L3-controle-apres-relance.txt)

### L1 - Liste de clôture
```
docker exec froid-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status          # [Kafka] quorum présent, 3 votants
docker exec froid-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:19092 | grep -E "^kafka-"    # [Kafka] 3 brokers actifs (id 4/A, 5/B, 6/C)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list                               # [Kafka] topics métier présents
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions   # [Kafka] vide
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --unavailable-partitions        # [Kafka] vide : toute partition a un leader
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-min-isr-partitions      # [Kafka] vide
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --exclude-internal               # [Kafka] ISR complet (4,5,6) et leaders préférés partout
docker exec froid-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --all-groups            # [Kafka] offsets des groupes conservés (relecture possible)
docker exec froid-kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --entity-type topics --entity-name <topic>   # [Kafka] aucun réglage de démonstration restant (5 topics)
```

### L2 - Arrêt et relance
```
docker compose -f docker-compose-froid.yml stop                       # [hôte] arrête les 6 conteneurs (brokers d'abord, puis contrôleurs) ; volumes conservés
docker compose -f docker-compose-froid.yml ps -a --format "{{.Name}} · {{.Status}}"   # [hôte] 6 conteneurs Exited
docker volume ls --filter name=projet-froid                          # [hôte] les 6 volumes nommés sont toujours là
docker compose -f docker-compose-froid.yml start                      # [hôte] relance les 6 conteneurs (healthy en 21 s)
docker logs <conteneur> --since <heure de relance> 2>&1 | grep -E 'already formatted|Formatting'   # [hôte] « All of the log directories are already formatted. » sur les 6 nœuds
```

### L3 - Contrôle après relance
```
docker exec froid-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --replication     # [Kafka] Lag 0 ; nouveau leader du quorum : contrôleur 2
docker exec froid-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic mesures-brutes         # [Kafka] offsets identiques à avant l'arrêt : données conservées
docker exec froid-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh ... --topic etat-capteurs --from-beginning ...              # [Kafka] état compacté et tombstone intacts
docker logs froid-kafka-1 --until <heure de relance> 2>&1 | tail -4                                                            # [hôte] « shut down completed » : arrêt propre malgré le code 137
```

---

## Validation du 25/10/2026 sur un second poste (critères 7 et 8 : preuves/H1-segments-25-10.txt, H2-complement-*, H3-*, I6-*, I7-*, J-*)

Cluster reconstruit à partir du dépôt sur un poste où il n'existait pas (Docker 29.8.2, 12 CPU, 11,5 Gio pour Docker, mêmes identifiants `.env`). Topics de démonstration `demo-*` uniquement, tous supprimés à la fin ; `etat-capteurs` et `tp-migration` laissés intacts pour refaire H2 et I. Heures en UTC.

### Reconstruction (phases C à E, prérequis de H et I) et relance après crash (preuves/J-relance-apres-crash.txt)
```
docker compose -f docker-compose-froid.yml config --quiet && docker compose -f docker-compose-froid.yml up -d --wait   # [hôte] 18:46 : formatage des 6 volumes ; crash du moteur WSL de Docker Desktop à 18:47:55 -> 6 conteneurs Exited (255)
docker compose -f docker-compose-froid.yml start                                       # [hôte] relance sans reformatage, 6 services healthy en 86 s, « already formatted » sur les contrôleurs
docker compose -f docker-compose-froid.yml up -d --wait --force-recreate kafka-1 kafka-2 kafka-3   # [hôte] docker logs figé pour les brokers : conteneurs recréés, volumes conservés, « already formatted » x3
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh ... --create ...                # [Kafka] les 4 topics de la phase D (preuves/D-topics.txt)
printf '...5 lignes du sujet...\n' | docker exec -i froid-kafka-1 /opt/kafka/bin/kafka-console-producer.sh ... --topic mesures-brutes ... acks=all, idempotence   # [hôte -> Kafka] phase E
```

### H1, réglages par défaut, H3 rétention (preuves/H1-segments-25-10.txt, H3-retention.txt)
```
docker exec froid-kafka-1 bash -c 'cd /opt/kafka && bin/kafka-configs.sh ... --describe --all --entity-type brokers --entity-name 4 | grep ...'   # [Kafka] H1 + défauts : retention.check 300000, segment.delete.delay 60000, cleaner.backoff 15000...
docker exec froid-kafka-1 /opt/kafka/bin/kafka-topics.sh ... --create --topic test-segment-bytes ... --config segment.bytes=1024   # [Kafka] refusé : « Value must be at least 1048576 » (rien de créé)
docker exec froid-kafka-1 grep "Starting log cleanup" /opt/kafka/logs/server.log        # [Kafka] 18:54:44 : contrôles de rétention à 18:55:14 + k x 300 s
(script de validation hors dépôt : commandes de la doc 10, H3, calées sur le contrôle de 19:05:14)   # [hôte] demo-retention (RF 3, leader 4, retention.ms=60000, segment.ms=20000) : 5 valeurs à 19:03:43, 9.2 à 19:04:54, segment 0 supprimé à 19:05:14, début 0 -> 5, topic supprimé
```

### Compléments H2 et I, cas RF 1 (preuves/H2-complement-segment-actif.txt, I6-throttle-et-retour-arriere.txt, I7-rf1-min-isr-effectif.txt)
```
...kafka-topics.sh --create --topic demo-compaction ... ; kafka-configs.sh --alter ... --add-config segment.ms=20000,min.cleanable.dirty.ratio=0.01   # [Kafka] rejeu de H2 : mêmes sorties que le sujet
printf 'CAP-001:9.5\n' | ...kafka-console-producer.sh ... --topic demo-compaction       # [hôte -> Kafka] 9.0 disparaît, 9.2 (offset 3) et 9.5 (offset 4) restent ; topic supprimé
...kafka-reassign-partitions.sh ... --reassignment-json-file /tmp/plan-demo.json --execute --throttle 1048576   # [Kafka] demo-migration (20 000 mesures) : quotas posés puis retirés par --verify
...kafka-reassign-partitions.sh ... --reassignment-json-file /tmp/retour-demo.json --execute ; --verify      # [Kafka] retour arrière : RF 1, offsets 6600/6120/7280 identiques ; topic supprimé
docker exec froid-controller-1 ... kafka-dump-log.sh --cluster-metadata-decoder ... | grep <TopicId>   # [Kafka] leaders rendus à 19:28:31 par le rééquilibrage automatique
...kafka-topics.sh --create --topic demo-rf1 --replication-factor 1 ; producteur acks=all ; --delete   # [Kafka] RF 1 sous min.insync.replicas=2 : voir I7
```

### J - Tableau de bord, maintenance progressive, panne disque, arrêt-relance (preuves/J-*.txt)
```
bash scripts/tableau-de-bord.sh                                                        # [hôte] 1re version boguée (second verdict erroné), corrigée puis complétée ; relevés propres : J-tableau-de-bord.txt
docker stop froid-kafka-3 ; ... ; docker start froid-kafka-3                           # [hôte] maintenance : arrêt en 7 s, 65 URP, écriture acks=all acceptée, 0 URP à T+75 s
docker exec froid-kafka-1 /opt/kafka/bin/kafka-leader-election.sh ... --election-type PREFERRED --all-topic-partitions   # [Kafka] 21 partitions rendues à leur leader préféré
docker stop froid-kafka-3 ; docker rm froid-kafka-3 ; docker volume rm projet-froid_kafka-3   # [hôte] panne disque SIMULÉE du broker 6
docker compose -f docker-compose-froid.yml up -d --wait kafka-3                         # [hôte] volume neuf formaté (même cluster.id, nouvel UUID de répertoire), 0 URP en 83 s, données recopiées
docker compose -f docker-compose-froid.yml stop ; docker compose -f docker-compose-froid.yml start   # [hôte] arrêt-relance : brokers tués à 10 s (code 137), « no clean shutdown file » ; offsets identiques
docker compose -f docker-compose-froid.yml stop -t 120 ; docker compose -f docker-compose-froid.yml start   # [hôte] arrêt propre en 27 s (codes 143), « Skipping recovery of 65 logs … clean shutdown file was found » (preuves/J-arret-propre-stop-t120.txt)
```

## Problèmes rencontrés et corrections

| # | Phase | Problème observé | Diagnostic / correction |
|---|---|---|---|
| 1 | 0 | La consigne citait `Documentation/sujet-TP.pdf` et `reference/docker-compose-3-brokers.yml`, qui n'existent pas. | Fichiers réels : `../Documetation/sujet tp/MCCI_..._V2.pdf` et `../Documetation/réf compose/docker-compose-3-brokers.yml`. Lus à cet endroit, sans modification. |
| 2 | 0 | Ancien cluster présent (`kafka-1..3`, `setupkafka-controller-1..3`). | Conteneurs déjà arrêtés (Exited) : aucune mémoire occupée, aucune action. Le préfixe `froid-` évite le conflit de noms. |
| 3 | 0 | `pdftotext` : « Couldn't open text file » vers le dossier temporaire. | Le dossier n'existait pas encore : créé avec `mkdir -p`, puis extraction relancée. |
| 4 | C6 | Démarrage en 17 s au lieu des ≈ 4 min annoncées. | Pas un problème : image déjà en cache, 12 CPU. Les healthchecks ont validé l'état réel. |
| 5 | E | Le sujet prévient que le 1er consommateur peut ne rien afficher. | Une boucle de 3 essais était prête. Le 1er essai a suffi (5 messages lus). |
| 6 | E-F | Ligne d'information « The consumer rebalance protocol (KIP-848) is production-ready! » à chaque consommateur. | Simple information, retirée des preuves par `grep -v KIP-848`. |
| 7 | G2/G3 | `WARN ... Error connecting to node kafka-3:19092 ... UnknownHostException` avec une longue pile Java. | Normal : le conteneur arrêté n'a plus d'entrée DNS dans le réseau Docker, mais l'AdminClient connaît encore le broker 6 par les métadonnées. Laissé en G2 (preuve réelle), filtré en G3 pour la lisibilité. |
| 8 | G4 | Leaders non préférés pendant ≈ 5 min après la relance. | Comportement attendu (`auto.leader.rebalance.enable=true`, contrôle toutes les 300 s). Boucle d'attente : retour à T+293 s. |
| 9 | K | Les agrégats simulés (CAP-001 : 4 mesures, max 9,0) ne correspondent pas aux mesures réellement envoyées (5 mesures entre 09:00 et 09:05, max 9,4). | Valeurs volontairement illustratives, marquées `"source":"SIMULATION"`. Un vrai job Flink les calculerait. Signalé dans la documentation. |
| 10 | L2 | Codes de sortie 137 (et 143) au `stop`. | Le journal montre « shut down completed » avant la sortie de la JVM, et au redémarrage « 0 recovered » : l'arrêt était propre. |
| 11 | G2 | Un fichier temporaire `/tmp/urp.txt` a été écrit dans le `/tmp` de Git Bash (hors du projet). | Supprimé immédiatement après usage (`rm -f /tmp/urp.txt`). Les comptages suivants passent par des variables shell. |
| 12 | 25/10, C | Sur le second poste, crash du moteur WSL de Docker Desktop pendant le premier `up` (hôte à 0,4 Go de RAM libre) : 6 conteneurs `Exited (255)`. | `docker compose ... start` : relance sans reformatage. Puis `--force-recreate` des 3 brokers, dont `docker logs` restait figé (volumes conservés). Mémoire libérée en fermant des applications. |
| 13 | 25/10, J | La 1re version de `scripts/tableau-de-bord.sh` affichait un second verdict erroné après chaque indicateur sain (la fonction `verdict` renvoyait un code non nul, ce qui déclenchait la branche `\|\|`). | Fonction corrigée (`return 0`, tests en `if`/`else`), alignement des libellés accentués, puis ajout des contrôleurs en service et des réglages de démonstration. Les preuves J donnent les valeurs relevées par la 1re version, sans ses verdicts. |
| 14 | 25/10, J/L | `docker compose stop` : brokers tués au bout de 10 s (code 137) ; au redémarrage, « Recovering 65 logs … since no clean shutdown file was found ». | Pas de perte (offsets identiques). Les brokers mettent 12 à 14 s à s'arrêter sur ce poste. `stop -t 120` vérifié : arrêt propre et aucune vérification au redémarrage (`stop_grace_period` dans le Compose : proposé, non appliqué). |
| 15 | 25/10 | Bascules répétées du leader du quorum (19:28, 19:39-19:40, 20:07-20:08) : `NotControllerException` transitoires dans les journaux des contrôleurs. | Cause : hôte saturé en mémoire. Aucune partition touchée ; erreurs classées dans `preuves/J-*.txt`. |
| 16 | 25/10, I | `leader.imbalance.per.broker.percentage`, cité par la doc 05, n'existe plus en 4.3.1. | Doc 05 et doc 10 corrigées. |
