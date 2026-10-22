# 8. Recette

Cluster `VJxGZOKwQIay5zzcsB_t3w`, recette effectuée le 22/10/2026 entre 12:48 et 13:07.

| N° | Critère de recette | Statut | Preuve : ce qu'elle montre | Fichier(s) |
|---|---|---|---|---|
| 1 | Trois contrôleurs KRaft et un quorum de métadonnées sain | **OK** | `describe --status` : `LeaderId: 3`, `CurrentVoters` 1, 2, 3 avec les UUID de `.env`. Quorum toujours sain après G (LeaderId 3) et après stop/start (LeaderId 2). | `preuves/C6-quorum.txt`, `L1-liste-cloture.txt`, `L3-controle-apres-relance.txt` |
| 2 | Trois brokers connectés, identifiants 4, 5 et 6 | **OK** | `describe --replication` : 4, 5 et 6 en `Observer`, Lag 0. `kafka-broker-api-versions` : `id: 4 rack: A`, `id: 5 rack: B`, `id: 6 rack: C`. | `preuves/C6-quorum.txt`, `L1-liste-cloture.txt` |
| 3 | Topics métier répliqués, politique de durabilité justifiée | **OK** | 4 topics `ReplicationFactor: 3`, `min.insync.replicas=2`, rétention 7 j / 30 j / compaction. Origine de min ISR expliquée. Calculs de dimensionnement. | `preuves/D-topics.txt`, `D-configs.txt` ; `Documentation/02`, `04` |
| 4 | Écriture de mesures avec clé, lecture par deux groupes distincts | **OK** | CAP-001 : une seule partition (1), offsets 0 à 4 dans l'ordre. `supervision-froid` et `analyse-qualite` lisent chacun les 7 mesures. LAG 3 → 0. `rejeu-incident` relit les 10 mesures. | `preuves/E2-lecture.txt`, `E3-ordre-par-cle.txt`, `F1-*.txt`, `F2-*.txt`, `F3-*.txt` |
| 5 | Sortie puis retour d'une réplique dans l'ISR après une panne | **OK** | ISR {4,5,6} → {4,5} (G2) → {4} (G3) → {4,5,6} à T+12 s (G4). Leaders préférés revenus à T+293 s. | `preuves/G1-etat-initial.txt`, `G2-*.txt`, `G4-restauration.txt`, `G4-controle-final.txt` |
| 6 | Refus des écritures quand `min.insync.replicas=2` n'est plus satisfait | **OK** | `NOT_ENOUGH_REPLICAS` ×3 puis `NotEnoughReplicasException: Messages are rejected since there are fewer in-sync replicas than required.` Offset de fin inchangé. | `preuves/G3-deux-brokers-arretes.txt` |
| 7 | Rétention, compaction et réaffectation de répliques | **OK** | `kafka-dump-log.sh` (lot `count: 3`, `producerId`/`sequence`). Compaction : offsets 0 et 1 de CAP-001 supprimés, 2 et 3 conservés, tombstone `CAP-002 null`, réglages de démo retirés. Réaffectation RF 1 → 3, `--verify` completed, élection PREFERRED puis « Valid replica already elected ». | `preuves/H1-segments.txt`, `H2-*.txt`, `I1-I2-generate.txt`, `I3-I4-reaffectation.txt`, `I5-election-preferee.txt` ; `json/` |
| 8 | Plan de supervision, de sécurité et de remise en service argumenté | **OK** | Tableau de métriques, maintenance progressive, reprise disque, sécurité. `stop`/`start` : « All of the log directories are already formatted. » sur les 6 nœuds, données intactes. | `Documentation/06` ; `preuves/L1-liste-cloture.txt`, `L2-stop-start.txt`, `L3-controle-apres-relance.txt` |

**Bilan : 8 critères sur 8 OK.**

Liste de clôture (phase L), vérifiée avant l'arrêt : quorum présent et 3 votants ✔ · 3 brokers actifs ✔ · topics métier présents ✔ · aucune partition sous-répliquée, sous le min ISR ou sans leader ✔ · ISR complet et leaders préférés partout ✔ · groupes avec offsets conservés ✔ · aucun réglage de démonstration restant ✔ · changements consignés dans `Command.md` ✔ · preuves dans `preuves/` ✔.
