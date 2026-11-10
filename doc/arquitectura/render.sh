#!/bin/bash
# Exporta las vistas C4 a doc/img/c4/*.png con fondo blanco. Requiere: npm i likec4 ; pip install pillow
set -e
AQUI="$(cd "$(dirname "$0")" && pwd)"
npx likec4 export png "$AQUI" -o "$AQUI/../img/c4" --light --flat
python3 - "$AQUI/../img/c4" <<'PY'
import sys, glob
from PIL import Image
for f in glob.glob(sys.argv[1] + "/*.png"):
    im = Image.open(f).convert("RGBA")
    fondo = Image.new("RGBA", im.size, (255, 255, 255, 255))
    fondo.alpha_composite(im)
    fondo.convert("RGB").save(f)
PY
