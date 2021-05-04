#!/bin/bash

echo 'Remove the "tag_build" option from setup.cfg'
read -p "Press enter to continue"

rm -rf build dist
python3 -m pip install --upgrade build twine
python3 -m build
ls dist/
python3 -m twine upload 'dist/*'

