# Compose generation

`deployment/bundle.py` derives a complete Compose document from the existing
`env/docker-compose.yaml`. It replaces fixed container names, runtime mounts,
ports, execution profiles and image settings using the validated site config.
Generated files belong outside the checkout. Keep a single maintained base
recipe; do not copy site credentials or generated deployments into this folder.
