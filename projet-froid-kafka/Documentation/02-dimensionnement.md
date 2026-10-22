# 2. Dimensionnement (phase A.2 et A.3)

## 2.1 Tableau de calculs

Unités décimales : 1 Mo = 10⁶ octets, 1 Go = 10⁹ octets. Formule simplifiée du cours (p. 53) : **stockage brut = débit × rétention × facteur de réplication**.

| # | Grandeur | Calcul | Résultat | Hypothèse utilisée |
|---|---|---|---|---|
| 1 | Messages/s, moyenne | 500 × 1 | **500 msg/s** | 500 capteurs, 1 mesure/s/capteur |
| 2 | Messages/s, pointe | 500 × 2 | **1 000 msg/s** | Pic = 2 × débit moyen |
| 3 | Débit entrant moyen | 500 × 200 | **100 000 o/s = 0,1 Mo/s** | 200 o/message, clé et valeur comprises |
| 4 | Débit entrant de pointe | 1 000 × 200 | **200 000 o/s = 0,2 Mo/s** | Lignes 2 et 3 |
| 5 | Messages sur 7 jours | 500 × 604 800 s | 302 400 000 messages | Rétention brute 7 jours = 604 800 s |
| 6 | Données brutes sur 7 jours | 100 000 × 604 800 | **60,48 Go** | Ligne 3 × rétention ; débit **moyen** |
| 7 | Stockage avec réplication | 60,48 × 3 | **181,44 Go** | `replication.factor=3` |
| 8 | Capacité cible avec marge | 181,44 × 1,30 | **235,87 Go** (≈ 236 Go) | Marge de 30 % (hypothèse du TP), appliquée au **total** |
| 9 | Par broker | 235,87 / 3 | ≈ 78,6 Go | Une réplique de chaque partition par broker (3 brokers, RF=3) |
| 10 | Écritures disque totales en pointe | 0,2 × 3 | 0,6 Mo/s | Chaque message est écrit sur 3 brokers |
| 11 | Lecture en pointe (3 applications) | 0,2 × 3 | 0,6 Mo/s, plus 0,4 Mo/s de réplication | Tableau de bord, alertes, analyse ; 2 suiveurs par partition |

**Contrôle de cohérence** : ligne 7 = 3 × ligne 6 (181,44 = 3 × 60,48) ✔ ; la marge s'applique au total répliqué (ligne 8 = 1,3 × ligne 7) ✔.

Les topics `alertes-temperature` et `mesures-agregees` (30 jours) ne sont pas chiffrés par le sujet. Si on suppose un agrégat par capteur et par minute (500/60 ≈ 8,3 msg/s de 200 o), on obtient environ 1,67 ko/s × 2 592 000 s × 3 ≈ 13 Go répliqués. Les alertes sont rares et donc négligeables. C'est une hypothèse de notre groupe, à confirmer avec le client.

## 2.2 Écarts avec le poste de laboratoire (A.3)

| Ressource | Besoin cible (production) | Poste de laboratoire (relevé le 22/10/2026) |
|---|---|---|
| Disque | ≈ 236 Go, idéalement 3 disques dédiés de ≈ 80 Go | Docker Desktop (WSL2) sur C: : **20 Go libres** sur 238 Go |
| Mémoire | Plusieurs Go de heap par broker, plus le cache de pages du système | Docker : 8,16 Go pour **6 nœuds** limités à 512 Mo de heap (réglage de laboratoire) |
| CPU | Faible (0,2 Mo/s), mais TLS et compression en consomment | 12 processeurs logiques partagés |
| Isolation | 3 hôtes dans 3 zones physiques | **Un seul hôte** : la panne d'un « rack » est simulée par `docker stop` |

Le poste ne peut pas stocker 7 jours de trafic, et le TP ne l'exige pas : on utilise quelques messages de test. La configuration (rétention, réplication, min ISR) est en revanche celle de la cible.

**Ce que la formule ignore :**

- **Compression** (`compression.type` côté producteur : lz4, zstd). Des JSON répétitifs se compressent souvent de 3 à 5 fois, ce qui fait baisser le besoin réel.
- **Format des lots** : en-têtes de lot et d'enregistrement, horodatages. Mesure réelle de la phase H (`preuves/H1-segments.txt`) : la clé fait 7 o et la valeur JSON 86 o, soit 93 o utiles. Sur disque, un lot de 3 mesures occupe **367 o (≈ 122 o/mesure)** et un lot d'une seule mesure **163 o**. L'hypothèse de 200 o/message est donc prudente pour ce format, mais le surcoût par message dépend fortement du regroupement en lots.
- **Index** (`.index`, `.timeindex`) : préalloués à 10 Mo par segment actif (observé en phase H), petits une fois le segment fermé.
- **Segments** : la rétention supprime des segments entiers. Le segment actif n'est jamais supprimé, et le volume réel dépasse donc légèrement 7 jours (jusqu'à `segment.bytes` = 1 Go ou `segment.ms` = 7 jours de plus par partition).
- **Fichiers internes** : `__consumer_offsets` (50 partitions), journal des métadonnées KRaft côté contrôleurs, checkpoints, snapshots.
- **Marge d'exploitation** : réaffectation de partitions (copie temporaire), croissance du parc de capteurs, rattrapage après panne, seuil d'alerte disque (ne jamais dépasser ≈ 70-80 % d'occupation).

## 2.3 Réponses aux questions de la phase A

**A-Q1. Pourquoi dimensionne-t-on les performances sur le pic, mais le stockage sur le débit moyen ?**
Les performances (réseau, CPU, I/O, nombre de partitions) doivent absorber le **débit instantané** : si le cluster ne tient pas le pic, la latence monte, les producteurs accumulent du retard et les consommateurs aussi. Le stockage, lui, est une **intégrale dans le temps** : sur 7 jours, les pics sont compensés par les creux, et le volume conservé dépend du débit moyen × rétention. Dimensionner le disque sur le pic surestimerait le besoin d'un facteur 2.

**A-Q2. Quelles hypothèses pourraient rendre les estimations fausses ?**
- Taille réelle des messages (format JSON plus verbeux, en-têtes, ou au contraire compression efficace).
- Fréquence non uniforme : capteurs qui envoient plus souvent en cas d'alarme, rafales au redémarrage d'une passerelle (rattrapage du tampon local).
- Pic supérieur à 2× (incident généralisé, reconnexion massive).
- Croissance du parc (plus de 500 capteurs, nouveaux sites).
- Volume des topics de sortie et des fichiers internes, non pris en compte.
- Rétention prolongée à la demande du métier (enquête, audit).
- Une durée réelle supérieure à 7 jours à cause du segment actif (voir plus haut).

**A-Q3. Pourquoi 500 capteurs ne signifient-ils pas 500 partitions ?**
La partition est l'unité de **parallélisme** et d'**ordre**, pas l'unité d'identification d'un capteur. La clé `sensorId` est hachée : plusieurs capteurs partagent une partition, et toutes les mesures d'un capteur restent ensemble et ordonnées (cours 2.2.2). Le nombre de partitions se choisit selon le débit cible et le nombre maximal de consommateurs actifs d'un même groupe (cours 2.2.6 : une partition n'est lue que par un consommateur du groupe). À 0,2 Mo/s en pointe, 6 partitions suffisent largement, et elles permettent jusqu'à 6 consommateurs parallèles par groupe (2 leaders par broker). 500 partitions × RF 3 = 1 500 répliques à gérer : plus de fichiers ouverts, plus de métadonnées, des élections plus longues et des lots plus petits, sans aucun gain de débit utile.
