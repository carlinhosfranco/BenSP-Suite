MAIN_PATH="$(pwd)"
APP_PATH="$MAIN_PATH/"apps""
DEDUP_DIR="$APP_PATH/"dedup""
FERRET_DIR="$APP_PATH/"ferret""
FERRET_DIR_LOG="$APP_PATH/"ferret"/temp"
DEDUP_DIR_LOG="$APP_PATH/"dedup"/temp"
DPI_DIR="$APP_PATH/"dpi""

BENSP_DIR="$MAIN_PATH"

export BENSP_DIR
export DEDUP_DIR
export FERRET_DIR
export FERRET_DIR_LOG
export DEDUP_DIR_LOG
export DPI_DIR
export PATH=${PATH}:${BENSP_DIR}/bin