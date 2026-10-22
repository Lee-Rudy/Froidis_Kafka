# 5. Vérifications et preuves (phases E à I)

Toutes les sorties citées sont des extraits réels du cluster du 22/10/2026. Les fichiers complets sont dans `preuves/` et les commandes dans `Command.md`.

---

## 5.1 Phase E : envoyer des mesures de test

**Fait.** Cinq mesures du sujet envoyées par pipe dans le producteur console (`--command-property acks=all --command-property enable.idempotence=true`, clé avant `:`), puis relues avec partition et offset. Le premier essai de lecture a suffi.

```
Partition:2  Offset:0  CAP-003  {"eventId":"E005",...,"temperatureC":3.8}
Partition:1  Offset:0  CAP-001  {"eventId":"E001",...,"temperatureC":4.2}
Partition:1  Offset:1  CAP-001  {"eventId":"E003",...,"temperatureC":8.7}
Partition:1  Offset:2  CAP-001  {"eventId":"E004",...,"temperatureC":9.0}
Partition:3  Offset:0  CAP-002  {"eventId":"E002",...,"temperatureC":5.1}
```
Preuves : `E1-production.txt`, `E2-lecture.txt`, `E3-ordre-par-cle.txt`.

**Point de contrôle E : atteint.** Les trois mesures de CAP-001 sont dans la partition 1, aux offsets 0, 1 et 2.

**E-Q1. Les messages de CAP-001 restent-ils sur la même partition, dans l'ordre ?** Oui. Deux mesures de plus (E006, E007) envoyées avec la même clé arrivent en partition 1 aux offsets 3 et 4. La lecture de la seule partition 1 rend E001, E003, E004, E006, E007 dans l'ordre d'envoi. Par la suite, E008, E011 et E013 ont aussi atterri en partition 1 (offsets 5 à 7).

**E-Q2. Les offsets sont-ils globaux au topic ? La clé sert-elle seulement à afficher le capteur ?** Non aux deux questions. `kafka-get-offsets.sh` donne une fin par partition (`1:5`, `2:1`, `3:1`, les autres à 0) : l'offset est une position **dans une partition**, et chaque partition commence à 0. La clé détermine la **partition** (hachage murmur2 de la clé modulo le nombre de partitions), donc l'ordre par capteur. Elle sert aussi à la **compaction** (dernière valeur par clé, phase H).

**E-Q3. `acks=all` peut-il créer des doublons métier si l'application envoie deux fois le même événement ?** Oui. `acks=all` porte sur la durabilité, et l'idempotence ne supprime que les doublons **techniques** : une même requête retransmise par le même producteur (même `producerId`, même `sequence`). Si l'application renvoie volontairement l'événement, c'est un nouvel enregistrement avec un nouveau numéro de séquence, et il est stocké deux fois. On l'a vu en phase H : chaque lancement du producteur console reçoit un nouveau `producerId` (1000, 0, 1, 2, 4), et un renvoi depuis un autre lancement ne serait donc jamais dédupliqué. Remède : un identifiant métier (`eventId`) filtré en aval (Flink), ou les transactions et l'exactly-once de bout en bout.

---

## 5.2 Phase F : consommateurs, groupes et relecture

**Fait** (`F1-deux-groupes.txt`, `F2-lag-groupe-arrete.txt`, `F3-reprise-et-rejeu.txt`) :
1. `supervision-froid` et `analyse-qualite` lisent chacun les **7** mesures alors présentes : les 5 du sujet et les 2 de la question E-1.
2. `analyse-qualite` est arrêté (`--max-messages` valide ses offsets, comme Ctrl-C), puis 3 nouvelles mesures sont produites (CAP-001, CAP-003, CAP-002) :
```
Consumer group 'analyse-qualite' has no active members.
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
analyse-qualite mesures-brutes  1          5               6               1
analyse-qualite mesures-brutes  2          1               2               1
analyse-qualite mesures-brutes  3          1               2               1
(partitions 0, 4, 5 : LAG 0)
```
3. Relancé sans `--from-beginning`, le groupe lit **exactement** E009, E008 et E010, et son LAG revient à 0 sur toutes les partitions.
4. Le nouveau groupe `rejeu-incident` avec `--from-beginning` relit les **10** mesures. `supervision-froid`, qui n'a pas été relancé, garde un LAG de 3.

**Point de contrôle F : atteint.** LAG total de 3, ramené à 0 après la relance, et un nouveau groupe relit tout.

**F-1. Deux groupes distincts ou deux consommateurs du même groupe ?** Deux **groupes distincts** reçoivent chacun **tout** le flux, avec des offsets indépendants : c'est la diffusion (tableau de bord et analyse). Deux consommateurs du **même groupe** se **partagent** les partitions : chaque partition est attribuée à un seul membre, et chaque message n'est traité qu'une fois par le groupe. C'est la mise à l'échelle d'une application. Au-delà de 6 membres pour `mesures-brutes`, les consommateurs en trop restent inactifs.

**F-2. Que mesurent `CURRENT-OFFSET`, `LOG-END-OFFSET` et `LAG` ? Où est stocké `CURRENT-OFFSET` ?**
`CURRENT-OFFSET` est le prochain offset que le groupe lira, c'est-à-dire le dernier offset validé. `LOG-END-OFFSET` est l'offset du prochain message qui sera écrit dans la partition. `LAG = LOG-END-OFFSET − CURRENT-OFFSET` est le nombre de messages en attente pour ce groupe. Les offsets validés sont stockés **dans Kafka**, dans le topic interne compacté `__consumer_offsets` (50 partitions, RF 3, relevé en G1), dont la clé est (groupe, topic, partition).

**F-3. Pourquoi le nouveau groupe ne relit-il pas dans l'ordre global d'envoi ?** Kafka ne garantit l'ordre qu'**à l'intérieur d'une partition**. Le consommateur reçoit des lots de plusieurs partitions dans un ordre qui dépend des requêtes de lecture. Ici, `rejeu-incident` a rendu toute la partition 3 (CAP-002), puis la 1 (CAP-001), puis la 2 (CAP-003). L'ordre **par capteur** est respecté, l'ordre global non, et c'est exactement le besoin exprimé.

---

## 5.3 Phase G : tolérance aux pannes

**Fait** (`G1-etat-initial.txt` à `G4-controle-final.txt`). Pendant tout le scénario, le quorum est resté disponible (LeaderId 3), ce qui confirme l'intérêt des rôles séparés.

| Moment | Leaders (p0 → p5 de `mesures-brutes`) | ISR | Écriture `acks=all` |
|---|---|---|---|
| **État initial** (12:52) | 5, 6, 4, 5, 6, 4 | {4,5,6} partout, 0 partition sous-répliquée | acceptée (phases E-F) |
| **Broker 6 arrêté** (`docker stop froid-kafka-3`) | 5, **4**, 4, 5, **4**, 4 | {4,5} partout ; **65** partitions sous-répliquées (50 `__consumer_offsets` + 15 métier) | **acceptée** (E011, fin de partition 1 : 6 → 7) |
| **Brokers 6 et 5 arrêtés** (`docker stop froid-kafka-2`) | 4 partout | **{4}**, `Elr: 5` | **refusée** : 3 × `NOT_ENOUGH_REPLICAS`, puis `NotEnoughReplicasException` en **3 s** ; fin de partition 1 inchangée (7) |
| **Retour des brokers 5 et 6** (`docker start`, T+12 s) | 4 partout | {4,5,6} : 0 partition sous-répliquée à **T+12 s** | **acceptée** (E013, fin : 8) |
| **Six minutes plus tard** (leaders rétablis à T+293 s ≈ 4 min 53) | 5, 6, 4, 5, 6, 4 | {4,5,6} | non retestée sur ce topic ; écritures `acks=all` acceptées en phase K (`mesures-agregees`, `alertes-temperature`) |

Extrait de G3 :
```
Partition: 0  Leader: 4  Replicas: 5,6,4  Isr: 4  Elr: 5
WARN [Producer clientId=console-producer] Got error produce response ... on topic-partition mesures-brutes-1, retrying (2 attempts left). Error: NOT_ENOUGH_REPLICAS
ERROR Error when sending message to topic mesures-brutes with key: 7 bytes, value: 86 bytes with error:
org.apache.kafka.common.errors.NotEnoughReplicasException: Messages are rejected since there are fewer in-sync replicas than required.
```
Pendant G3, la **lecture** de la partition 1 restait possible (leader 4 vivant).

**Point de contrôle G : atteint.** Avec un broker arrêté, l'ISR a deux brokers et les écritures passent. Avec deux brokers arrêtés, on obtient `NotEnoughReplicasException`. Après la relance, aucune partition n'est sous-répliquée, et toutes les partitions métier sont revenues à leur leader préféré (`G4-controle-final.txt`).

**G-1. Pourquoi `acks=all` et `min.insync.replicas=2` protègent-ils les écritures en les refusant ?** `acks=all` signifie « confirme quand **toutes les répliques de l'ISR** ont le message ». Si l'ISR se réduit au seul leader, « toutes » ne veut plus dire qu'une copie : un message confirmé serait perdu si ce dernier disque mourait. `min.insync.replicas=2` impose un plancher. Le leader refuse l'écriture (`NOT_ENOUGH_REPLICAS`) tant qu'il n'y a pas au moins 2 répliques synchronisées. Le producteur reçoit une **erreur explicite** au lieu d'une confirmation trompeuse : on choisit la cohérence plutôt que la disponibilité en écriture.

**G-2. Pourquoi une partition sous-répliquée n'est-elle pas toujours indisponible ?** Sous-répliquée signifie ISR < RF (2 < 3). Tant qu'il reste un leader, la partition est **lisible**. Tant que l'ISR est ≥ `min.insync.replicas`, elle est **accessible en écriture**. En G2, les 65 partitions étaient sous-répliquées et pourtant pleinement utilisables. Elle ne devient indisponible en écriture que sous le min ISR (G3), et totalement indisponible sans leader (`OfflinePartitionsCount`).

**G-3. Que signifie `Elr: 5` ?** ELR veut dire **Eligible Leader Replicas** (KIP-966, actif par défaut en Kafka 4.x). Quand l'ISR passe sous `min.insync.replicas`, la réplique retirée de l'ISR, ici le broker 5 arrêté en dernier, est mémorisée comme **éligible** : sous le min ISR, plus aucune écriture n'a pu être validée, donc elle possède tout ce qui a été confirmé (jusqu'au high watermark). Si le broker 4 tombait à son tour, le 5 pourrait redevenir leader **sans perte et sans élection impure**. Lors de l'exercice 2, la colonne était vide parce que l'ISR était complet : aucune réplique n'avait quitté l'ISR sous le seuil.

**G-4. Pourquoi `__consumer_offsets` apparaît-il dans les partitions sous-répliquées ?** C'est un topic interne ordinaire, avec 50 partitions et RF 3 (`offsets.topic.replication.factor=3`), réparti sur les trois brokers. Chaque broker héberge donc une réplique de chacune des 50 partitions. L'arrêt de n'importe quel broker les rend toutes sous-répliquées : 50 + 15 = 65.

---

## 5.4 Phase H : segments, rétention et compaction

### H1 : le stockage sur disque (`H1-segments.txt`)

```
log.dirs=/var/lib/kafka/data      log.segment.bytes=1073741824      log.roll.hours=168
-rw-r--r-- 1 appuser appuser 10485760 Oct 22 08:49 00000000000000000000.index
-rw-r--r-- 1 appuser appuser     1121 Oct 22 08:54 00000000000000000000.log
-rw-r--r-- 1 appuser appuser 10485756 Oct 22 08:49 00000000000000000000.timeindex
baseOffset: 0 lastOffset: 2 count: 3 baseSequence: 0 lastSequence: 2 producerId: 1000 producerEpoch: 0 partitionLeaderEpoch: 0 ... size: 367
| offset: 0 ... keySize: 7 valueSize: 86 sequence: 0 key: CAP-001 payload: {"eventId":"E001",...}
| offset: 1 ... keySize: 7 valueSize: 86 sequence: 1 key: CAP-001 payload: {"eventId":"E003",...}
| offset: 2 ... keySize: 7 valueSize: 86 sequence: 2 key: CAP-001 payload: {"eventId":"E004",...}
baseOffset: 3 lastOffset: 4 count: 2 ... producerId: 0 ...
baseOffset: 6 lastOffset: 6 count: 1 ... producerId: 2 ... partitionLeaderEpoch: 1   (E011, écrite pendant G2 avec le leader 4)
```

**H-Q1. Pourquoi les trois mesures forment-elles un seul lot (`count: 3`) ?** Le producteur regroupe les enregistrements destinés à la **même partition** dans un lot (`batch.size`, `linger.ms`) envoyé en une seule requête. Les cinq lignes sont arrivées d'un coup par le pipe, et les trois mesures de CAP-001, toutes destinées à la partition 1, ont rempli le même lot. Le lot est l'unité d'écriture, de réplication et de compression : 367 o pour 3 mesures, contre 163 o pour un lot d'une seule.

**H-Q2. À quoi servent `producerId` et `sequence` ?** Ils servent à l'**idempotence**. Le broker garde, par `producerId` et par partition, le dernier numéro de séquence reçu. Un lot retransmis après une perte d'acquittement porte la même séquence et il est **ignoré**. Un trou dans la séquence est détecté comme une erreur d'ordre. Chaque lancement du producteur console a obtenu un nouveau `producerId` (1000, 0, 1, 2, 4), et la séquence repart de 0 à chaque fois. `partitionLeaderEpoch` passe de 0 à 1 pour E011 : le leader a changé en G2.

**H-Q3. Quel lien entre offset, segment et politique de rétention ?** Une partition est une suite de **segments**, chacun nommé par son premier offset (`00000000000000000000.log`), avec son index offset → position et son index temps → offset. Un seul segment est actif. Il est fermé quand il atteint `segment.bytes` (1 Gio) ou `segment.ms` / `log.roll.hours` (7 j). La rétention `delete` supprime des **segments entiers fermés** dont le message le plus récent dépasse `retention.ms`. Le *log start offset* avance alors, mais les offsets restants ne sont jamais renumérotés. Avec des segments de 7 jours et une rétention de 7 jours, on garde en pratique jusqu'à environ 14 jours de données dans le pire cas (voir `02`).

### H2 : le topic compacté (`H2-compaction-avant.txt`, `H2-compaction-apres.txt`, `H2-tombstone-et-retrait.txt`)

Réglages de démonstration : `segment.ms=20000`, `min.cleanable.dirty.ratio=0.01`. Cinq valeurs envoyées à 12:59:59, puis `CAP-001:9.2` à 13:00:26 (plus de 20 s après), et relecture à 13:01:33.

| Avant | Une minute après `CAP-001:9.2` |
|---|---|
| `Partition:1 Offset:0 CAP-001 4.2` | `Partition:0 Offset:0 CAP-002 5.1` |
| `Partition:1 Offset:1 CAP-001 8.7` | `Partition:2 Offset:0 CAP-003 3.8` |
| `Partition:1 Offset:2 CAP-001 9.0` | `Partition:1 Offset:2 CAP-001 9.0` |
| `Partition:2 Offset:0 CAP-003 3.8` | `Partition:1 Offset:3 CAP-001 9.2` |
| `Partition:0 Offset:0 CAP-002 5.1` | |

Sur disque (broker 4) : l'ancien segment `00000000000000000000.log` est réduit à **78 o** et ne contient plus que l'offset 2 (`Non-consecutive offsets … -1 is followed by 2`). Le segment actif `00000000000000000003.log` contient 9.2. Le tombstone `CAP-002:NULL` est ensuite relu comme `Partition:0 Offset:1 CAP-002 null`. Les réglages de démonstration ont été **retirés** : il ne reste que `cleanup.policy=compact` et `min.insync.replicas=2`.

**Point de contrôle H : atteint.** Segment lu avec `kafka-dump-log.sh`, offsets 0 et 1 de CAP-001 disparus et les autres inchangés, tombstone visible, réglages retirés.

**H2-Q1. Quelles valeurs ont disparu ? Les offsets ont-ils été renumérotés ?** Les valeurs 4.2 (offset 0) et 8.7 (offset 1) de CAP-001 ont disparu. Les offsets restants **gardent leur numéro** (2 et 3) : la compaction crée des trous et ne renumérote jamais. Les positions validées des consommateurs restent donc valides.

**H2-Q2. Pourquoi 9.0 reste-t-elle alors que 9.2 est plus récente ?** Le nettoyeur ne touche **jamais le segment actif**. Il n'a nettoyé que le segment fermé (offsets 0 à 2), et dans ce segment la dernière valeur de CAP-001 est 9.0. La valeur 9.2 est dans le segment actif `…0003`. 9.0 ne disparaîtra qu'au prochain passage, après la fermeture de ce segment. La compaction garantit « **au moins** la dernière valeur par clé », pas « exactement une valeur », et son moment n'est pas garanti.

**H2-Q3. Pourquoi `compact` ne remplace-t-il pas la conservation de l'historique ?** La compaction **détruit les valeurs intermédiaires** : on ne pourrait plus savoir que CAP-001 est passé par 8.7 puis 9.0, ni pendant combien de temps, ce qui rend impossible la détection « > 8 °C pendant 2 minutes » a posteriori, le rejeu ou l'analyse d'incident. Les tombstones finissent eux aussi par disparaître (`delete.retention.ms`, 24 h par défaut). D'où les deux topics : `mesures-brutes` (historique, 7 j) et `etat-capteurs` (dernier état).

---

## 5.5 Phase I : réaffectation et leaders préférés

**Fait** (`I1-I2-generate.txt`, `I3-I4-reaffectation.txt`, `I5-election-preferee.txt` ; JSON dans `json/`). Les fichiers JSON ont été écrits en LF / UTF-8 dans `json/`, puis copiés dans le conteneur par pipe (`docker exec -i … 'cat > /tmp/x.json' < json/x.json`), ce qui évite les problèmes d'encodage d'un fichier Windows.

| Étape | `tp-migration` p0 | p1 | p2 |
|---|---|---|---|
| Création (RF 1) | Leader 5, Replicas 5 | Leader 6, Replicas 6 | Leader 4, Replicas 4 |
| Après `--execute` / `--verify` (RF 3) | Leader **5**, Replicas 4,5,6 | Leader **6**, Replicas 5,6,4 | Leader **4**, Replicas 6,4,5 |
| Après élection PREFERRED | Leader **4** | Leader **5** | Leader **6** |
| Seconde élection | `Valid replica already elected for partitions tp-migration-2, tp-migration-1, tp-migration-0` | | |

Le bloc « Current partition replica assignment » est conservé dans `json/retour-arriere-migration.json`. La proposition de `--generate` (une seule réplique, déplacée) n'a pas été utilisée : le plan écrit à la main augmente le RF à 3.

**Point de contrôle I : atteint.** `ReplicationFactor: 3`, leaders égaux au premier broker de chaque liste, et la seconde élection répond « Valid replica already elected ».

**I-Q1. Pourquoi garder le plan initial ? À quoi servent `--generate`, `--execute` et `--verify` ?** Le plan initial est le **retour arrière** : en cas de problème (saturation réseau ou disque, mauvais placement), on le relance avec `--execute` pour revenir à l'état d'origine. `--generate` lit l'affectation actuelle et **propose** une répartition sur les brokers donnés, sans rien modifier. `--execute` **lance** la copie des répliques (asynchrone) et réaffiche l'affectation courante à sauvegarder. `--verify` **contrôle** que chaque partition a terminé et retire les quotas de réplication (« Clearing broker-level throttles ») posés avec `--throttle`.

**I-Q2. Pourquoi le premier broker de `Replicas` n'est-il pas leader dès la fin de la réaffectation ?** La réaffectation change la **liste** des répliques, pas le leader. Changer de leader est une élection distincte, et Kafka n'en déclenche pas tant que le leader actuel reste dans la nouvelle liste et dans l'ISR. Les leaders 5, 6 et 4 étaient toujours valides, ils sont restés en place. Le premier de la liste n'est que le **leader préféré**.

**I-Q3. Que répond Kafka à une seconde élection ? Quel réglage l'aurait fait seul ?** « *Valid replica already elected* » : le leader préféré est déjà leader, il n'y a rien à faire. `auto.leader.rebalance.enable=true` (défaut, relevé sur le broker 4) avec `leader.imbalance.check.interval.seconds=300` aurait rétabli seul les leaders préférés au contrôle suivant, comme en G4 (retour à T+293 s), tant que le déséquilibre dépasse `leader.imbalance.per.broker.percentage` (10 %).
