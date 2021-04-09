function activate_venv() {
    # try to activate a "regular .venv"
    source .venv/bin/activate || \
        # if it fails: warn the user and exit
        ( echo ""; echo "ERROR: failed to activate virtual environment .venv! ask for advice on #dev "; return 1 )
}

activate_venv && (
    echo ""
    echo "************************************************************************************"
    echo "Successfuly activated the virtual environment; you are now using this python:"
    echo $(which python)
    echo "************************************************************************************"
    echo ""
)
export PYTHONPATH="$PYTHONPATH:$(pwd)"
