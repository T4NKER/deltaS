#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ] && [ "${1:-}" != "--force" ]; then
    echo "Refusing to overwrite existing .env. Use --force to regenerate." >&2
    exit 1
fi

rand_secret() {
    python3 -c 'import secrets; print(secrets.token_urlsafe(48))'
}

gen_rsa_pair() {
    local priv pub
    priv=$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 2>/dev/null)
    pub=$(echo "$priv" | openssl rsa -pubout 2>/dev/null)
    printf '%s\n---\n%s' "$priv" "$pub"
}

esc_pem() {
    awk 'BEGIN{ORS="\\n"}{print}' <<< "$1"
}

echo "Generating watermark / token / JWT random secrets ..."
WATERMARK_SECRET=$(rand_secret)
TOKEN_SIGNING_SECRET=$(rand_secret)
TOKEN_SALT=$(rand_secret)
JWT_SECRET_KEY=$(rand_secret)
SELLER_JWT_SECRET_KEY=$(rand_secret)
DELIVERY_TOKEN_SECRET_KEY=$(rand_secret)

echo "Generating Marketplace RSA-2048 keypair (RS256 JWT) ..."
mp_pair=$(gen_rsa_pair)
MARKETPLACE_JWT_PRIVATE_KEY=$(echo "$mp_pair" | sed -n '/-----BEGIN PRIVATE KEY-----/,/-----END PRIVATE KEY-----/p')
MARKETPLACE_JWT_PUBLIC_KEY=$(echo "$mp_pair" | sed -n '/-----BEGIN PUBLIC KEY-----/,/-----END PUBLIC KEY-----/p')

echo "Generating Seller metadata-signing RSA-2048 keypair (RSA-PSS) ..."
sm_pair=$(gen_rsa_pair)
SELLER_METADATA_SIGNING_PRIVATE_KEY=$(echo "$sm_pair" | sed -n '/-----BEGIN PRIVATE KEY-----/,/-----END PRIVATE KEY-----/p')
SELLER_METADATA_SIGNING_PUBLIC_KEY=$(echo "$sm_pair" | sed -n '/-----BEGIN PUBLIC KEY-----/,/-----END PUBLIC KEY-----/p')

cat > .env <<EOF
WATERMARK_SECRET=${WATERMARK_SECRET}
TOKEN_SIGNING_SECRET=${TOKEN_SIGNING_SECRET}
TOKEN_SALT=${TOKEN_SALT}
JWT_SECRET_KEY=${JWT_SECRET_KEY}
SELLER_JWT_SECRET_KEY=${SELLER_JWT_SECRET_KEY}
DELIVERY_TOKEN_SECRET_KEY=${DELIVERY_TOKEN_SECRET_KEY}
MARKETPLACE_JWT_PRIVATE_KEY="$(esc_pem "$MARKETPLACE_JWT_PRIVATE_KEY")"
MARKETPLACE_JWT_PUBLIC_KEY="$(esc_pem "$MARKETPLACE_JWT_PUBLIC_KEY")"
SELLER_METADATA_SIGNING_PRIVATE_KEY="$(esc_pem "$SELLER_METADATA_SIGNING_PRIVATE_KEY")"
SELLER_METADATA_SIGNING_PUBLIC_KEY="$(esc_pem "$SELLER_METADATA_SIGNING_PUBLIC_KEY")"
ALLOW_INSECURE_DEFAULTS=true
EOF

chmod 600 .env

echo "Wrote .env (mode 0600)."
echo "Now run: docker compose --profile testing up -d --build"
