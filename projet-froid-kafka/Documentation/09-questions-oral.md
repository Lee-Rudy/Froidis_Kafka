# 9. Questions à défendre à l'oral

**1. Pourquoi trois contrôleurs KRaft et non un seul, alors que le cluster a trois brokers ?**
Le nombre de contrôleurs ne dépend pas du nombre de brokers, mais de la **tolérance aux pannes du quorum**. Raft exige une majorité : avec 1 contrôleur, sa perte arrête toute gestion des métadonnées (élections de leaders, ISR, création de topics), c'est un point unique de défaillance. Avec 3, la majorité est 2 et on tolère la perte d'un contrôleur. 2 contrôleurs ne tolèrent rien (majorité = 2) et 4 pas plus que 3 : on choisit un nombre impair. Séparés des brokers, les contrôleurs sont restés disponibles pendant toute la phase G.

**2. En quoi `acks=all` ne suffit-il pas sans `min.insync.replicas` ?**
`acks=all` attend toutes les répliques **de l'ISR**, et l'ISR peut se réduire au seul leader. Avec `min.insync.replicas=1` (le défaut de Kafka), un message serait alors confirmé alors qu'il n'existe qu'en un exemplaire, et la perte de ce broker le ferait disparaître. `min.insync.replicas=2` fixe le plancher : en G3, avec ISR = {4}, Kafka a **refusé** l'écriture (`NotEnoughReplicasException`) au lieu de faire une fausse promesse. Les deux réglages ne protègent qu'ensemble.

**3. Pourquoi une partition peut-elle être sous-répliquée tout en restant lisible et accessible en écriture ?**
Sous-répliquée veut dire ISR < RF. Pour lire, il suffit d'un leader. Pour écrire en `acks=all`, il suffit que l'ISR soit ≥ `min.insync.replicas`. En G2, 65 partitions étaient sous-répliquées (ISR = 2 < 3), et pourtant l'écriture E011 a été acceptée et lue. C'est voulu : RF 3 avec min ISR 2 laisse une marge d'une panne.

**4. Pourquoi `__consumer_offsets` apparaît-il parfois dans les partitions sous-répliquées ?**
C'est un topic comme les autres, interne, avec 50 partitions et RF 3, où Kafka stocke les offsets validés des groupes. Ses répliques sont sur les trois brokers : quand un broker s'arrête, ses 50 partitions perdent une réplique de l'ISR. En G2 : 50 partitions + 15 métier = 65.

**5. Une réplication sur trois brokers dispense-t-elle de sauvegardes et de fsync dans tous les scénarios de panne ?**
Non. La réplication couvre la perte **isolée** d'un broker. Elle copie aussi les erreurs logiques (suppression de topic, mauvaise rétention, données corrompues) et ne protège pas d'un sinistre corrélé (site, rançongiciel). Par défaut, Kafka acquitte dès que le message est dans le **cache mémoire** des répliques, sans fsync. Une coupure simultanée des trois brokers peut donc perdre des messages confirmés. Les sauvegardes, l'archivage et l'indépendance réelle des zones restent nécessaires (`06`).

**6. Pourquoi la clé `sensorId` compte-t-elle pour l'ordre des mesures ?**
La clé est hachée pour choisir la partition. Même clé, même partition, et l'ordre est garanti **dans une partition**. Toutes les mesures de CAP-001 sont en partition 1, offsets 0 à 7 dans l'ordre d'envoi (E). Sans clé, les mesures d'un capteur seraient dispersées et Flink pourrait voir 9,0 °C avant 8,7 °C, ce qui fausserait la règle « pendant 2 minutes ». La clé sert aussi à la compaction (`etat-capteurs`).

**7. Pourquoi Kafka garde-t-il les messages après leur lecture ?**
Kafka est un **journal**, pas une file qui se vide : la lecture ne fait qu'avancer l'offset du groupe, stocké à part dans `__consumer_offsets`. Cela permet à plusieurs groupes de lire le même flux indépendamment (`supervision-froid`, `analyse-qualite`), à un consommateur arrêté de reprendre où il en était (LAG 3 → 0) et à un nouveau groupe de **rejouer** l'historique (`rejeu-incident`, 10 mesures). La suppression ne dépend que de la rétention (7 jours) ou de la compaction.

**8. Que perd-on avec la compaction, et que garde-t-on ?**
On garde **au moins la dernière valeur de chaque clé**, avec son offset d'origine. On perd les **valeurs intermédiaires** : 4.2 et 8.7 de CAP-001 ont disparu, donc l'historique et la durée d'une dérive. Les tombstones finissent aussi par disparaître. On garde temporairement plus que la dernière valeur, puisque le segment actif n'est pas compacté (9.0 et 9.2 coexistent), et le moment du nettoyage n'est pas garanti. D'où un topic compacté pour l'état et un topic à rétention pour l'historique.

**9. À quoi servent `--generate`, `--execute` et `--verify` pendant une réaffectation ?**
`--generate` affiche l'affectation **actuelle**, à garder comme plan de retour arrière (`json/retour-arriere-migration.json`), et **propose** une nouvelle répartition, sans rien modifier. `--execute` **lance** la copie des répliques selon le plan, de façon asynchrone et éventuellement limitée en débit (`--throttle`). `--verify` **vérifie** que chaque partition a terminé et **retire les quotas** de réplication. Ensuite, on rétablit les leaders préférés par une élection PREFERRED, ou on attend le rééquilibrage automatique.

**10. Quels changements faudrait-il pour transformer ce laboratoire en service industriel ?**
- Trois hôtes dans trois zones réellement indépendantes, et des contrôleurs sur des hôtes distincts.
- Disques dédiés dimensionnés (≈ 80 Go/broker selon `02`, plus la croissance), heap de plusieurs Go et cache de pages.
- TLS sur tous les listeners, SASL et ACL par application, secrets gérés, réseaux segmentés.
- Supervision (Prometheus, Grafana, alertes du tableau de `06`) et astreinte.
- Sauvegarde des configurations et identifiants, archivage longue durée (stockage objet), éventuellement un cluster de secours (MirrorMaker 2).
- Procédures écrites et testées (maintenance progressive, reprise disque, montée de version), gestion de la configuration (Ansible / Strimzi) et préproduction.
- Producteurs réels avec tampon local en cas d'indisponibilité, et job Flink pour les alertes à la place de la simulation.
