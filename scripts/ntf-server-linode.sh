#!/bin/bash

# <UDF name="api_token" label="Linode API token - enable Linode to create tags with server address, fingerprint and version. Note: minimal permissions token should have are read/write access to `linodes` (to create tags) and `domains` (to add A record for the third level domain if FQDN is provided)." default="" />
# TODO review
# <UDF name="fqdn" label="FQDN (Fully Qualified Domain Name) - provide third level domain name (e.g. smp.example.com). If provided use `smp://fingerprint@FQDN` as server address in the client. If FQDN is not provided use `smp://fingerprint@IP` instead." default="" />
# <UDF name="apns_key_id" label="APNS key ID." default="" />

# Log all stdout output to stackscript.log
exec &> >(tee -i /var/log/stackscript.log)

# Uncomment next line to enable debugging features
# set -xeo pipefail

cd $HOME

# https://superuser.com/questions/1638779/automatic-yess-to-linux-update-upgrade
# https://superuser.com/questions/1412054/non-interactive-apt-upgrade
sudo DEBIAN_FRONTEND=noninteractive \
  apt-get \
  -o Dpkg::Options::=--force-confold \
  -o Dpkg::Options::=--force-confdef \
  -y --allow-downgrades --allow-remove-essential --allow-change-held-packages \
  update

sudo DEBIAN_FRONTEND=noninteractive \
  apt-get \
  -o Dpkg::Options::=--force-confold \
  -o Dpkg::Options::=--force-confdef \
  -y --allow-downgrades --allow-remove-essential --allow-change-held-packages \
  dist-upgrade

# TODO install unattended-upgrades
sudo DEBIAN_FRONTEND=noninteractive \
  apt-get \
  -o Dpkg::Options::=--force-confold \
  -o Dpkg::Options::=--force-confdef \
  -y --allow-downgrades --allow-remove-essential --allow-change-held-packages \
  install jq

# Add firewall
echo "y" | ufw enable

# Open ports
ufw allow ssh
ufw allow https
ufw allow 5223

# Increase file descriptors limit
echo 'fs.file-max = 1000000' >> /etc/sysctl.conf
echo 'fs.inode-max = 1000000' >> /etc/sysctl.conf
echo 'root soft nofile unlimited' >> /etc/security/limits.conf
echo 'root hard nofile unlimited' >> /etc/security/limits.conf

# Download latest release
bin_dir="/opt/simplex-notifications/bin"
binary="$bin_dir/ntf-server"
mkdir -p $bin_dir
curl -L -o $binary https://github.com/simplex-chat/simplexmq/releases/latest/download/ntf-server-ubuntu-20_04-x86-64
chmod +x $binary

# / Add to PATH
cat > /etc/profile.d/simplex.sh << EOF
#!/bin/bash

export PATH="$PATH:$bin_dir"

EOF
# Add to PATH /

# Source and test PATH
source /etc/profile.d/simplex.sh
ntf-server --version

# Initialize server
init_opts=()

ip_address=$(curl ifconfig.me)
init_opts+=(--ip $ip_address)

[[ -n "$FQDN" ]] && init_opts+=(-n $FQDN)

smp-server init "${init_opts[@]}"

# Server fingerprint
fingerprint=$(cat /etc/opt/simplex-notifications/fingerprint)

# Determine server address to specify in welcome script and Linode tag
# ! If FQDN was provided and used as part of server initialization, server's certificate will not pass validation at client
# ! if client tries to connect by server's IP address, so we have to specify FQDN as server address in Linode tag and
# ! in welcome script regardless of creation of A record in Linode
# ! https://hackage.haskell.org/package/x509-validation-1.6.10/docs/src/Data-X509-Validation.html#validateCertificateName
if [[ -n "$FQDN" ]]; then
  server_address=$FQDN
else
  server_address=$ip_address
fi

# Set up welcome script
on_login_script="/opt/simplex-notifications/on_login.sh"

# / Welcome script
cat > $on_login_script << EOF
#!/bin/bash

fingerprint=\$1
server_address=\$2

cat << EOF2
********************************************************************************

SimpleX notifications server address: smp://\$fingerprint@\$server_address
Check server status with: systemctl status ntf-server

To keep this server secure, the UFW firewall is enabled.
All ports are BLOCKED except 22 (SSH), 443 (HTTPS), 5223 (notifications server).

********************************************************************************
To stop seeing this message delete line - bash /opt/simplex-notifications/on_login.sh - from /root/.bashrc
EOF2

EOF
# Welcome script /

chmod +x $on_login_script
echo "bash $on_login_script $fingerprint $server_address" >> /root/.bashrc

# Create A record and update Linode's tags
if [[ -n "$API_TOKEN" ]]; then
  if [[ -n "$FQDN" ]]; then
    domain_address=$(echo $FQDN | rev | cut -d "." -f 1,2 | rev)
    domain_id=$(curl -H "Authorization: Bearer $API_TOKEN" https://api.linode.com/v4/domains \
    | jq --arg da "$domain_address" '.data[] | select( .domain == $da ) | .id')
    if [[ -n $domain_id ]]; then
      curl \
        -s -H "Content-Type: application/json" \
        -H "Authorization: Bearer $API_TOKEN" \
        -X POST -d "{\"type\":\"A\",\"name\":\"$FQDN\",\"target\":\"$ip_address\"}" \
        https://api.linode.com/v4/domains/${domain_id}/records
    fi
  fi

  version=$(ntf-server --version | cut -d ' ' -f 3-)

  curl \
    -s -H "Content-Type: application/json" \
    -H "Authorization: Bearer $API_TOKEN" \
    -X PUT -d "{\"tags\":[\"$server_address\",\"$fingerprint\",\"$version\"]}" \
    https://api.linode.com/v4/linode/instances/$LINODE_ID
fi

# / Create systemd service
cat > /etc/systemd/system/ntf-server.service << EOF
[Unit]
Description=SimpleX notifications server

[Service]
Environment="APNS_KEY_FILE=/etc/opt/simplex-notifications/AuthKey.p8"
Environment="APNS_KEY_ID=$APNS_KEY_ID"
Type=simple
ExecStart=/bin/sh -c "exec $binary start >> /var/opt/simplex-notifications/ntf-server.log 2>&1"
KillSignal=SIGINT
Restart=always
RestartSec=10
LimitNOFILE=1000000
LimitNOFILESoft=1000000

[Install]
WantedBy=multi-user.target

EOF
# Create systemd service /

# Start systemd service
chmod 644 /etc/systemd/system/ntf-server.service
sudo systemctl enable ntf-server
# ! APNS key file has to be created manually
# sudo systemctl start ntf-server

# Reboot Linode to apply upgrades
# sudo reboot
