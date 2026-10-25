#!/usr/bin/env bash
# Génère le PDF A4 du document 10 à partir du Markdown (pandoc, puis Chrome sans interface).
# Usage (Git Bash, depuis n'importe quel dossier) : bash Documentation/pdf/generer-pdf.sh
# Chrome tourne avec un profil temporaire : il n'interfère pas avec le navigateur déjà ouvert.
set -e
ICI="$(cd "$(dirname "$0")" && pwd)"
NOM="10-criteres-7-8-guide-H-I-J"
CHROME="${CHROME:-/c/Program Files/Google/Chrome/Application/chrome.exe}"
HTML="$ICI/$NOM.html"
PROFIL="$(mktemp -d)"

pandoc "$ICI/../$NOM.md" -f gfm -t html5 --standalone --embed-resources \
  --css "$ICI/style-impression.css" --metadata pagetitle="Critères 7 et 8 - FROIDIS" -o "$HTML"

timeout 120 "$CHROME" --headless=new --disable-gpu --no-first-run --no-pdf-header-footer \
  --user-data-dir="$(cygpath -w "$PROFIL")" \
  --print-to-pdf="$(cygpath -w "$ICI/$NOM.pdf")" "$(cygpath -w "$HTML")" 2>/dev/null

rm -rf "$PROFIL" "$HTML"
echo "PDF : $ICI/$NOM.pdf"
