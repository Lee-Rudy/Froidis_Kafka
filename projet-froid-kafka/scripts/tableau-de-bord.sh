#!/usr/bin/env bash
# Tableau de bord de supervision FROIDIS (phase J), version laboratoire.
# Chaque indicateur du plan de supervision (Documentation/10, section 4.1) est relevé par
# son équivalent en ligne de commande, puis comparé à son seuil : OK, AVERTISSEMENT ou CRITIQUE.
# Lecture seule : aucune commande ne modifie le cluster.
#
# Usage (terminal hôte, dossier projet-froid-kafka) :
#   bash scripts/tableau-de-bord.sh            # journaux des 15 dernières minutes
#   bash scripts/tableau-de-bord.sh 60m        # autre fenêtre pour les journaux
# Code retour : 0 si tout est OK, 1 si au moins un avertissement, 2 si au moins une alerte critique.

export MSYS_NO_PATHCONV=1                      # Git Bash : ne pas convertir /opt/kafka en chemin Windows
export LC_ALL=C.UTF-8                          # longueurs comptées en caractères (libellés accentués)
FENETRE=${1:-15m}
OUTIL="docker exec froid-kafka-1 /opt/kafka/bin"
BS="--bootstrap-server kafka-1:19092"
BROKERS="froid-kafka-1 froid-kafka-2 froid-kafka-3"
NOEUDS="froid-controller-1 froid-controller-2 froid-controller-3 $BROKERS"
PIRE=0

verdict() {   # verdict <0|1|2> <indicateur> <détail> ; renvoie toujours 0
  local etiquette=("OK           " "AVERTISSEMENT" "CRITIQUE     ")
  local marge=$(( 34 - ${#2} ))
  if [ "$marge" -lt 1 ]; then marge=1; fi
  printf '[%s] %s%*s%s\n' "${etiquette[$1]}" "$2" "$marge" "" "$3"
  if [ "$1" -gt "$PIRE" ]; then PIRE=$1; fi
  return 0
}

compte() { grep -c "Partition:" || true; }

echo "=== Tableau de bord FROIDIS : $(date '+%d/%m/%Y %H:%M:%S') (journaux : $FENETRE) ==="

# 1. Contrôleur actif et quorum (ActiveControllerCount, raft-metrics, Lag de describe --replication)
#    CurrentVoters liste les votants configurés, même arrêtés : on compte donc aussi les contrôleurs en service.
QUORUM=$($OUTIL/kafka-metadata-quorum.sh $BS describe --status 2>/dev/null)
LEADER=$(awk '/^LeaderId:/ {print $2}' <<< "$QUORUM")
VOTANTS=$(grep '^CurrentVoters:' <<< "$QUORUM" | grep -o '"id"' | wc -l)
RETARD=$(awk '/^MaxFollowerLag:/ {print $2}' <<< "$QUORUM")
SAINS=0
for c in froid-controller-1 froid-controller-2 froid-controller-3; do
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null)" = "healthy" ] && SAINS=$((SAINS + 1))
done
if [ -z "$LEADER" ] || [ "$LEADER" = "-1" ] || [ "$SAINS" -lt 2 ]; then
  verdict 2 "Contrôleur actif (quorum)" "leader '${LEADER:-aucun}', $SAINS contrôleur(s) en service sur 3 : plus de majorité"
elif [ "$SAINS" -lt 3 ] || [ "$VOTANTS" -lt 3 ]; then
  verdict 1 "Contrôleur actif (quorum)" "leader $LEADER, $SAINS contrôleur(s) en service sur 3 : plus aucune marge"
elif [ "${RETARD:-0}" -gt 1000 ]; then
  verdict 1 "Contrôleur actif (quorum)" "leader $LEADER, 3 votants en service, retard max $RETARD (> 1000)"
else
  verdict 0 "Contrôleur actif (quorum)" "leader $LEADER, 3 votants en service, retard max $RETARD"
fi

# 2. Brokers enregistrés et non isolés (fenced)
ACTIFS=$($OUTIL/kafka-broker-api-versions.sh $BS 2>/dev/null | grep -c "isFenced: false")
if [ "$ACTIFS" -eq 3 ]; then
  verdict 0 "Brokers actifs" "3 / 3 (ids 4, 5, 6)"
elif [ "$ACTIFS" -ge 2 ]; then
  verdict 1 "Brokers actifs" "$ACTIFS / 3 : maintenance ou panne d'un broker"
else
  verdict 2 "Brokers actifs" "$ACTIFS / 3 : écritures acks=all refusées si l'ISR passe sous 2"
fi

# 3. Partitions sous-répliquées (UnderReplicatedPartitions)
URP=$($OUTIL/kafka-topics.sh $BS --describe --under-replicated-partitions 2>/dev/null | compte)
if [ "$URP" -eq 0 ]; then
  verdict 0 "Sous-réplication (URP)" "0 partition"
else
  verdict 1 "Sous-réplication (URP)" "$URP partition(s) avec ISR < RF : critique si > 5 min"
fi

# 4. Partitions au min ISR / sous le min ISR (UnderMinIsrPartitionCount, AtMinIsrPartitionCount)
AMIN=$($OUTIL/kafka-topics.sh $BS --describe --at-min-isr-partitions 2>/dev/null | compte)
SMIN=$($OUTIL/kafka-topics.sh $BS --describe --under-min-isr-partitions 2>/dev/null | compte)
if [ "$SMIN" -gt 0 ]; then
  # L'outil compare l'ISR à la valeur brute de min.insync.replicas : un topic de RF 1 (tp-migration en I1-I3) y figure
  # aussi, alors que le broker l'accepte (minimum effectif = min(min.insync.replicas, RF)).
  verdict 2 "Sous le min ISR" "$SMIN partition(s) : écritures acks=all refusées (sauf topic de RF 1)"
elif [ "$AMIN" -gt 0 ]; then
  verdict 1 "Sous le min ISR" "0, mais $AMIN partition(s) au min ISR : plus aucune marge"
else
  verdict 0 "Sous le min ISR" "0 partition (et 0 au min ISR)"
fi

# 5. Partitions sans leader (OfflinePartitionsCount)
OFF=$($OUTIL/kafka-topics.sh $BS --describe --unavailable-partitions 2>/dev/null | compte)
if [ "$OFF" -eq 0 ]; then
  verdict 0 "Partitions hors ligne" "0 partition"
else
  verdict 2 "Partitions hors ligne" "$OFF partition(s) sans leader : ni lecture ni écriture"
fi

# 6. Leaders préférés (équilibre de charge, rétabli toutes les 300 s ou par élection PREFERRED)
NONPREF=$($OUTIL/kafka-topics.sh $BS --describe --exclude-internal 2>/dev/null | awk '
  /Partition:/ { for (i = 1; i <= NF; i++) { if ($i == "Leader:") l = $(i+1); if ($i == "Replicas:") { split($(i+1), r, ","); p = r[1] } }
                 if (l != p) n++ }
  END { print n + 0 }')
if [ "$NONPREF" -eq 0 ]; then
  verdict 0 "Leaders préférés" "toutes les partitions métier"
else
  verdict 1 "Leaders préférés" "$NONPREF partition(s) hors leader préféré"
fi

# 7. Retard des consommateurs (records-lag-max, LAG par groupe)
LAGS=$($OUTIL/kafka-consumer-groups.sh $BS --describe --all-groups 2>/dev/null | awk '
  $1 != "GROUP" && NF >= 6 && $6 ~ /^[0-9]+$/ { lag[$1] += $6 }
  END { for (g in lag) printf "%s=%d ", g, lag[g] }')
if [ -z "$LAGS" ]; then
  verdict 0 "Retard des consommateurs (LAG)" "aucun groupe avec offsets validés"
else
  MAXLAG=$(tr ' ' '\n' <<< "$LAGS" | awk -F= 'NF == 2 && $2 > m { m = $2 } END { print m + 0 }')
  if [ "$MAXLAG" -le 1000 ]; then
    verdict 0 "Retard des consommateurs (LAG)" "$LAGS"
  else
    verdict 1 "Retard des consommateurs (LAG)" "$LAGS(seuil 1000 : vérifier la tendance)"
  fi
fi

# 8. Disque : occupation du volume de données et répertoires hors ligne (OfflineLogDirectoryCount)
for b in $BROKERS; do
  USE=$(docker exec "$b" df -P /var/lib/kafka/data 2>/dev/null | awk 'NR == 2 { sub("%", "", $5); print $5 }')
  TAILLE=$(docker exec "$b" du -sh /var/lib/kafka/data 2>/dev/null | cut -f1)
  if [ -z "$USE" ]; then verdict 2 "Disque $b" "conteneur injoignable"
  elif [ "$USE" -ge 90 ]; then verdict 2 "Disque $b" "${USE} % occupé, données Kafka $TAILLE"
  elif [ "$USE" -ge 80 ]; then verdict 1 "Disque $b" "${USE} % occupé, données Kafka $TAILLE"
  else verdict 0 "Disque $b" "${USE} % occupé, données Kafka $TAILLE"
  fi
done
DIRS=$($OUTIL/kafka-log-dirs.sh $BS --describe --broker-list 4,5,6 2>/dev/null | grep '^{')
NDIRS=$(grep -o '"logDir"' <<< "$DIRS" | wc -l)
NERR=$(grep -o '"error":"[^"]*"' <<< "$DIRS" | wc -l)
if [ "$NDIRS" -ge 3 ] && [ "$NERR" -eq 0 ]; then
  verdict 0 "Répertoires de journaux" "$NDIRS en ligne, 0 hors ligne"
else
  verdict 2 "Répertoires de journaux" "$NDIRS lus, $NERR en erreur"
fi

# 9. Réglages et topics de démonstration oubliés (liste de clôture de la phase L)
DEMO_CFG=$($OUTIL/kafka-configs.sh $BS --describe --entity-type topics --entity-name etat-capteurs 2>/dev/null \
  | grep -cE '^ *(segment\.ms|min\.cleanable\.dirty\.ratio)=' || true)
DEMO_TOP=$($OUTIL/kafka-topics.sh $BS --list 2>/dev/null | grep -c '^demo-' || true)
if [ "$DEMO_CFG" -eq 0 ] && [ "$DEMO_TOP" -eq 0 ]; then
  verdict 0 "Réglages de démonstration" "aucun sur etat-capteurs, aucun topic demo-*"
else
  verdict 1 "Réglages de démonstration" "$DEMO_CFG réglage(s) sur etat-capteurs, $DEMO_TOP topic(s) de démonstration"
fi

# 10. Journaux : nouvelles lignes ERROR ou FATAL dans la fenêtre
for n in $NOEUDS; do
  NB=$(docker logs --since "$FENETRE" "$n" 2>&1 | grep -cE '\] (ERROR|FATAL) ' || true)
  if [ "$NB" -eq 0 ]; then
    verdict 0 "Journal $n" "0 ERROR/FATAL"
  else
    verdict 1 "Journal $n" "$NB ERROR/FATAL : lire docker logs --since $FENETRE $n"
  fi
done

case $PIRE in
  0) BILAN="tout est OK" ;;
  1) BILAN="avertissement(s) à examiner" ;;
  *) BILAN="ALERTE CRITIQUE" ;;
esac
echo "=== Bilan : $BILAN ==="
exit $PIRE
