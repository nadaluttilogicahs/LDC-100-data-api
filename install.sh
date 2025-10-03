#!/bin/bash

# Configurazione
REPO_URL="git@github.com:nadaluttilogicahs/LDC-100-data-api.git"
APP_NAME="data-api"
APP_DIR="/opt/${APP_NAME}"
SERVICE_USER="lg58"
SERVICE_PORT="8000"
DATA_DIR="/home/lg58/LDC-100/data"

echo "🚀 Installazione ${APP_NAME}..."

# Verifica se è root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Esegui con sudo: sudo bash install.sh"
    exit 1
fi

# Installa dipendenze sistema
echo "📦 Installazione dipendenze sistema..."
apt update
apt install -y python3 python3-pip python3-venv git

# Crea directory app
echo "📁 Creazione directory ${APP_DIR}..."
mkdir -p ${APP_DIR}

# dai i permessi a lg58 PRIMA di clonare
chown ${SERVICE_USER}:${SERVICE_USER} ${APP_DIR}

cd ${APP_DIR}

# Clona repository
echo "📥 Clone repository..."
if [ -d ".git" ]; then
    echo "Repository già esistente, aggiornamento..."
    sudo -u ${SERVICE_USER} git pull
else
    sudo -u ${SERVICE_USER} git clone ${REPO_URL} .
fi

# Crea virtual environment
echo "🐍 Creazione virtual environment..."
sudo -u ${SERVICE_USER} python3 -m venv venv

# Installa requirements
echo "📚 Installazione requirements..."
sudo -u ${SERVICE_USER} ${APP_DIR}/venv/bin/pip install -r requirements.txt

# Imposta permessi
echo "🔒 Impostazione permessi..."
chown -R ${SERVICE_USER}:${SERVICE_USER} ${APP_DIR}
chmod -R 750 ${APP_DIR}

# Configura .env
if [ ! -f "${APP_DIR}/.env" ]; then
    echo "⚙️  Creazione file .env da .env.example..."
    sudo -u ${SERVICE_USER} cp ${APP_DIR}/.env.example ${APP_DIR}/.env
    
    # Sostituisci i valori di default
    sed -i "s|DATA_BASE_DIR=/path/to/your/data|DATA_BASE_DIR=${DATA_DIR}|g" ${APP_DIR}/.env
    sed -i "s|API_KEY=your-secret-api-key-here|API_KEY=ldc-100-secret-key|g" ${APP_DIR}/.env
    
    chmod 600 ${APP_DIR}/.env
    chown ${SERVICE_USER}:${SERVICE_USER} ${APP_DIR}/.env
    
    echo "⚠️  IMPORTANTE: Modifica ${APP_DIR}/.env con i tuoi valori!"
    echo "   Specialmente l'API_KEY per sicurezza."
else
    echo "ℹ️  File .env già esistente, non modificato."
fi

# Verifica che la directory dati esista
if [ ! -d "${DATA_DIR}" ]; then
    echo "⚠️  ATTENZIONE: La directory dati ${DATA_DIR} non esiste!"
    echo "   Assicurati che esista prima di avviare il servizio."
fi

# Crea systemd service
echo "🔧 Creazione systemd service..."
cat > /etc/systemd/system/${APP_NAME}.service <<EOF
[Unit]
Description=LDC-100 Data API Server
After=network.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment="PATH=${APP_DIR}/venv/bin"
EnvironmentFile=${APP_DIR}/.env
ExecStart=${APP_DIR}/venv/bin/uvicorn app.main:app --host 0.0.0.0 --port ${SERVICE_PORT}
Restart=always
RestartSec=10

# Logging
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Ricarica systemd
echo "🔄 Ricarica systemd..."
systemctl daemon-reload

# Abilita e avvia servizio
echo "▶️  Avvio servizio..."
systemctl enable ${APP_NAME}.service
systemctl start ${APP_NAME}.service

# Verifica stato
sleep 2
systemctl status ${APP_NAME}.service --no-pager

echo ""
echo "✅ Installazione completata!"
echo ""
echo "⚠️  RICORDA: "
echo "   - Modifica ${APP_DIR}/.env per configurare l'API_KEY"
echo "   - Verifica che ${DATA_DIR} esista e contenga i database"
echo ""
echo "Dopo aver modificato .env, riavvia con:"
echo "   sudo systemctl restart ${APP_NAME}"
echo ""
echo "Comandi utili:"
echo "  - Stato:    sudo systemctl status ${APP_NAME}"
echo "  - Log:      sudo journalctl -u ${APP_NAME} -f"
echo "  - Restart:  sudo systemctl restart ${APP_NAME}"
echo "  - Stop:     sudo systemctl stop ${APP_NAME}"
echo ""
echo "API disponibile su: http://$(hostname -I | awk '{print $1}'):${SERVICE_PORT}"