# 4. Configuration Kafka (phases C et D)

Environnement relevé le 22/10/2026 (`preuves/C2-versions.txt`) : Docker 29.1.3, Docker Compose v2.40.3-desktop.1, Docker Desktop sur WSL2 (Windows 11), image `apache/kafka:4.3.1` (`sha256:47dccc76b327…`). Ressources Docker : 12 CPU, 8,16 Go.

## 4.1 Identifiants (`.env`)

Générés par `kafka-storage.sh random-uuid` (`preuves/C3-uuid.txt`) :

| Variable | Valeur | Rôle |
|---|---|---|
| `CLUSTER_ID` | `VJxGZOKwQIay5zzcsB_t3w` | Identité du cluster, écrite dans `meta.properties` de chaque volume. Un nœud formaté avec un autre id refuse de rejoindre le cluster. |
| `UUID_C1` | `gOtFIhrIS1CROaiIe0wMyQ` | Identifiant de répertoire (`directory.id`) du contrôleur 1 |
| `UUID_C2` | `qcQkn36mTYGryiSUju5IyA` | … du contrôleur 2 |
| `UUID_C3` | `KEjSrcxnTUSwHfG3A0pong` | … du contrôleur 3 |

Ces valeurs ne doivent plus changer : le premier formatage les a écrites dans les volumes. Les brokers ont reçu leur propre UUID de répertoire au formatage (4 : `UFZPjPoSN_xzen0yfUEOzg`, 5 : `Shq0wDNI4oN09V9ORfqmfA`, 6 : `cnRBS-aM7vX6e4nv-iNAQA`).

## 4.2 Fichiers `.properties` (`config/`)

| Paramètre | Contrôleurs (1, 2, 3) | Brokers (4, 5, 6) | Pourquoi |
|---|---|---|---|
| `process.roles` | `controller` | `broker` | Rôles séparés |
| `node.id` | 1, 2, 3 | 4, 5, 6 | Unique dans tout le cluster |
| `listeners` | `CONTROLLER://controller-N:9093` | `PLAINTEXT://kafka-N:19092` | Écoute sur le nom réseau Docker |
| `advertised.listeners` | — | `PLAINTEXT://kafka-N:19092` | Adresse renvoyée aux clients : doit être joignable par eux |
| `controller.listener.names` | `CONTROLLER` | `CONTROLLER` | Listener utilisé pour parler au quorum |
| `listener.security.protocol.map` | `CONTROLLER:PLAINTEXT` | `+ PLAINTEXT:PLAINTEXT` | Laboratoire uniquement (voir `06`) |
| `controller.quorum.bootstrap.servers` | les 3 contrôleurs | les 3 contrôleurs | Point d'entrée du quorum dynamique (KIP-853) |
| `inter.broker.listener.name` | — | `PLAINTEXT` | Réplication entre brokers |
| `broker.rack` | — | A, B, C | Répartition des répliques par zone |
| `log.dirs` | `/var/lib/kafka/data` | `/var/lib/kafka/data` | Monté sur un volume nommé |
| `num.partitions` / `default.replication.factor` | 6 / 3 | — | Défauts des topics créés sans option (lus par le contrôleur, qui crée les topics) |
| `min.insync.replicas` | 2 | — | Devient un **défaut dynamique du cluster** (voir D-3) |
| `offsets.topic.replication.factor` | — | 3 | `__consumer_offsets` répliqué 3 fois |
| `transaction.state.log.replication.factor` / `.min.isr` | — | 3 / 2 | Journal des transactions durable |
| `unclean.leader.election.enable` | — | `false` | Jamais de leader hors ISR (voir `06`) |
| `log.retention.hours` | — | 168 | Rétention par défaut : 7 jours |

Écart volontaire avec le cours (p. 48) : `num.partitions` et `default.replication.factor` sont dans les fichiers des **contrôleurs**. Kafka 4.3.1 avertit qu'ils seront ignorés côté broker en 5.0.

## 4.3 Fichier Compose (`docker-compose-froid.yml`)

- Projet `name: projet-froid` : réseau `projet-froid_default` et volumes `projet-froid_<service>`.
- Ancres YAML (inspirées du compose de référence) : `x-noeud` (image), `x-env-controleur` / `x-env-broker`, `x-sante-*` (healthchecks), `x-attente-quorum` (`depends_on`).
- **Aucune variable `KAFKA_*` de configuration** : seule `KAFKA_HEAP_OPTS` (taille de la JVM) est passée. Toute la configuration Kafka vient des `.properties`, montés en lecture seule.
- `${CLUSTER_ID}` est remplacé par Compose depuis `.env`. `$$CLUSTER_ID` devient `$CLUSTER_ID`, évalué par le `bash` du conteneur.
- Healthchecks : contrôleur sain quand `kafka-metadata-quorum.sh describe --status` affiche un `LeaderId`, broker sain quand `kafka-broker-api-versions.sh` répond. Les brokers attendent `service_healthy` des 3 contrôleurs.

## 4.4 Formatage

| Nœud | Commande lancée au démarrage du conteneur | Journal au 1er démarrage | Au redémarrage |
|---|---|---|---|
| Contrôleur | `kafka-storage.sh format --ignore-formatted --cluster-id $CLUSTER_ID --initial-controllers $QUORUM_INITIAL --config …/controller-N.properties` | `Formatting dynamic metadata voter directory /var/lib/kafka/data with metadata.version 4.3-IV0.` | `All of the log directories are already formatted.` |
| Broker | `kafka-storage.sh format --ignore-formatted --cluster-id $CLUSTER_ID --no-initial-controllers --config …/broker-N.properties` | `Formatting metadata directory /var/lib/kafka/data with metadata.version 4.3-IV0.` | idem |

`QUORUM_INITIAL = 1@controller-1:9093:<UUID_C1>,2@controller-2:9093:<UUID_C2>,3@controller-3:9093:<UUID_C3>` fixe les votants initiaux, chacun identifié par son id et son UUID de répertoire. `--ignore-formatted` rend la commande idempotente : elle ne refait rien si `meta.properties` existe (vérifié en phase L).

## 4.5 Démarrage et quorum (point de contrôle C)

`up -d --wait` a rendu la main en **17 s** (`preuves/C6-demarrage.txt`), avec 0 ligne ERROR dans les six journaux. Extrait de `preuves/C6-quorum.txt` :

```
ClusterId:              VJxGZOKwQIay5zzcsB_t3w
LeaderId:               3
CurrentVoters:          [{"id": 1, "directoryId": "gOtFIhrIS1CROaiIe0wMyQ", ...}, {"id": 2, "directoryId": "qcQkn36mTYGryiSUju5IyA", ...}, {"id": 3, "directoryId": "KEjSrcxnTUSwHfG3A0pong", ...}]
CurrentObservers:       [{"id": 5, ...}, {"id": 4, ...}, {"id": 6, ...}]

NodeId  DirectoryId             LogEndOffset  Lag  Status
3       KEjSrcxnTUSwHfG3A0pong  81            0    Leader
1       gOtFIhrIS1CROaiIe0wMyQ  81            0    Follower
2       qcQkn36mTYGryiSUju5IyA  81            0    Follower
5       Shq0wDNI4oN09V9ORfqmfA  81            0    Observer
4       UFZPjPoSN_xzen0yfUEOzg  81            0    Observer
6       cnRBS-aM7vX6e4nv-iNAQA  81            0    Observer
```

### Réponses aux questions de la phase C

**C-Q1.** Les trois UUID de `.env` apparaissent dans `CurrentVoters`. Les brokers sont des **observateurs** parce qu'ils ont `process.roles=broker` et ont été formatés avec `--no-initial-controllers` : ils répliquent le journal `__cluster_metadata` pour connaître l'état du cluster, mais ne votent pas et ne peuvent pas devenir leader du quorum. Cela garde le quorum petit (3 votants, donc des élections rapides) et isole les métadonnées de la charge de données.

**C-Q2.** La majorité de 3 est 2 : **un seul contrôleur** peut tomber. Avec deux contrôleurs perdus, il n'y a plus de leader du quorum : les brokers continuent de servir les partitions existantes avec leurs dernières métadonnées connues, mais aucun changement (création de topic, élection de leader, mise à jour d'ISR) n'est possible.

**C-Q3.** Le client se connecte d'abord au bootstrap, reçoit les métadonnées avec les adresses de `advertised.listeners`, puis se connecte **directement au leader de chaque partition** via cette adresse. Si elle est injoignable (nom non résolu, IP interne, port non publié), le bootstrap réussit, mais la production et la consommation échouent ensuite (délais dépassés, `UnknownHostException`). On l'a observé en phase G : l'adresse `kafka-3` du broker arrêté n'était plus résolue. Même problème pour la réplication si l'adresse inter-brokers est fausse.

## 4.6 Topics métier (phase D)

| Topic | Partitions | RF | min ISR | Politique | Justification |
|---|---|---|---|---|---|
| `mesures-brutes` | 6 | 3 | 2 | delete, `retention.ms=604800000` (7 j) | Rejeu des mesures sur 7 jours. 6 partitions = 2 leaders par broker, 6 consommateurs parallèles max par groupe. Clé `sensorId` pour l'ordre. |
| `mesures-agregees` | 3 | 3 | 2 | delete, `retention.ms=2592000000` (30 j) | Sorties Flink : débit faible, 1 leader par broker |
| `alertes-temperature` | 3 | 3 | 2 | delete, 30 j | Historique des alertes exigé : 30 jours. Perte inacceptable. |
| `etat-capteurs` | 3 | 3 | 2 | **compact** | Dernière valeur par clé (`sensorId`), pas de limite de temps : permet à un tableau de bord de se reconstruire au démarrage |

**Durabilité justifiée.** RF=3 et `min.insync.replicas=2` côté topic, `acks=all` et `enable.idempotence=true` côté producteur, `unclean.leader.election.enable=false` côté broker. **Panne couverte : un broker (une zone) sur trois.** Toute écriture confirmée est alors présente sur au moins 2 brokers et survit à la perte de l'un d'eux. Au-delà, Kafka **refuse** d'écrire (démontré en G3) au lieu de perdre des données. La garantie ne couvre pas une perte simultanée de plusieurs zones avant écriture sur disque, ni une erreur humaine (voir `06`).

### Tableau des leaders, répliques et ISR (D-1), relevé à la création (`preuves/D-topics.txt`)

| Topic | Partition | Leader | Répliques | ISR |
|---|---|---|---|---|
| mesures-brutes | 0 | 5 | 5,6,4 | 5,6,4 |
| mesures-brutes | 1 | 6 | 6,4,5 | 6,4,5 |
| mesures-brutes | 2 | 4 | 4,5,6 | 4,5,6 |
| mesures-brutes | 3 | 5 | 5,6,4 | 5,6,4 |
| mesures-brutes | 4 | 6 | 6,4,5 | 6,4,5 |
| mesures-brutes | 5 | 4 | 4,5,6 | 4,5,6 |
| mesures-agregees | 0 / 1 / 2 | 4 / 5 / 6 | 4,5,6 / 5,6,4 / 6,4,5 | complet |
| alertes-temperature | 0 / 1 / 2 | 4 / 5 / 6 | 4,5,6 / 5,6,4 / 6,4,5 | complet |
| etat-capteurs | 0 / 1 / 2 | 6 / 4 / 5 | 6,4,5 / 4,5,6 / 5,6,4 | complet |

**Les brokers se partagent-ils le leadership ?** Oui. Dans `mesures-brutes`, chacun mène exactement deux partitions (4 : p2 et p5 ; 5 : p0 et p3 ; 6 : p1 et p4), et chaque topic à 3 partitions donne un leader par broker. Au total, sur 15 partitions métier, chaque broker en mène 5. Le leader préféré est le premier broker de la liste des répliques, et les listes sont décalées en rotation.

**D-2. Nombre total de répliques :** (6 + 3 + 3 + 3) partitions × 3 = **45 répliques**, soit 15 par broker. Il faut y ajouter `__consumer_offsets` : 50 × 3 = 150 répliques, et `tp-migration` en phase I.

**D-3. Origine de `DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2`** (`preuves/D-configs.txt`) :
```
min.insync.replicas=2 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:min.insync.replicas=2, DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2, DEFAULT_CONFIG:min.insync.replicas=1}
```
La valeur vient du `min.insync.replicas=2` des **fichiers des contrôleurs**. En KRaft, le contrôleur l'enregistre dans les métadonnées comme configuration **par défaut de tous les brokers du cluster** (entité `brokers` par défaut). `kafka-configs.sh --describe --entity-type brokers --entity-default` le confirme : `min.insync.replicas=2 … {DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2}`. L'ordre de priorité est : configuration du topic (`DYNAMIC_TOPIC_CONFIG`), puis défaut dynamique du cluster, puis défaut de Kafka (1). Même sans `--config min.insync.replicas=2` à la création, un topic aurait hérité de 2 (c'est le cas de `__consumer_offsets` et `tp-migration`).

**Point de contrôle D : atteint.** Quatre topics avec `ReplicationFactor: 3` et `min.insync.replicas=2`, et deux leaders par broker dans `mesures-brutes`.

### Producteurs critiques

Configuration exigée et écrite explicitement, même si ce sont les défauts du client Java 4.3 :

```properties
acks=all
enable.idempotence=true
# conséquences par défaut : retries élevés, max.in.flight.requests.per.connection <= 5, delivery.timeout.ms=120000
key = sensorId
```
