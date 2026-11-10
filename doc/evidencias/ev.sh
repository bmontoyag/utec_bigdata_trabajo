# Uso: source doc/evidencias/ev.sh ; ev <nombre> <comando...>
# Ejecuta el comando y guarda la salida con encabezado (proyecto, fecha) en doc/evidencias/logs/<nombre>.log
export CLOUDSDK_ACTIVE_CONFIG_NAME=lumina
EVDIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)/logs"
ev() {
  local nombre="$1"; shift
  { echo "\$ $*"; echo "# proyecto: $(gcloud config get-value project 2>/dev/null) · $(date '+%Y-%m-%d %H:%M:%S %Z')"; echo
    eval "$@" 2>&1; local rc=$?; echo; echo "# código de salida: $rc"; } | tee "$EVDIR/$nombre.log"
}
