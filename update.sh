#!/bin/bash

# Configurazione
APP_NAME="data-api"
APP_DIR="/opt/${APP_NAME}"
SERVICE_USER="lg58"

echo "🔄 Aggiornamento ${APP_NAME}..."

# Verifica se è root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Esegui con sudo: sudo bash update.sh"
    exit 1
fi

# Vai nella directory app
cd ${APP_DIR}

# Ferma il servizio
echo "⏸️  Fermata servizio..."
systemctl stop ${APP_NAME}.service

# Backup .env
echo "💾 Backup configurazione..."
cp .env .env.backup

# Pull da Git
echo "📥 Download aggiornamenti..."
sudo -u ${SERVICE_USER} git pull

# Aggiorna requirements
echo "📚 Aggiornamento dipendenze..."
sudo -u ${SERVICE_USER} ${APP_DIR}/venv/bin/pip install -r requirements.txt

# Verifica se ci sono nuove variabili in .env.example
if ! diff -q .env.example .env.backup > /dev/null 2>&1; then
    echo "⚠️  .env.example è cambiato! Controlla se servono nuove variabili."
fi

# Riavvia servizio
echo "▶️  Riavvio servizio..."
systemctl start ${APP_NAME}.service

# Verifica stato
sleep 2
systemctl status ${APP_NAME}.service --no-pager

echo ""
echo "✅ Aggiornamento completato!"
echo ""
echo "Backup configurazione salvato in: ${APP_DIR}/.env.backup"