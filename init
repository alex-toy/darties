# exit on error
set -e

# try to create a .venv with poetry
(poetry install  && ../.venv/bin/activate ) || \
    # if it fails: warn the user (clean the .venv if it was partially created)
    (rm -rf .venv && echo "ERROR: failed to create the .venv : do it yourself!" && exit 1);


echo "*********************************************************"
echo "Successfully created the virtual environment! it is located at:"
echo "$(pwd)/.venv"
echo "In order to activate this venv:"
echo "$ source activate.sh"
echo "*********************************************************"
echo ""
