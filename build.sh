
pyinstaller --onefile --name openduck  openduck.py


create-dmg \
  --volname "DuckCLI" \
  --window-pos 200 120 \
  --window-size 600 400 \
  --icon-size 100 \
  --icon "duckcli" 200 190 \
  --hide-extension "duckcli" \
  "DuckCLI.dmg" \
  "dist/"


compile to c++ code
onefile
pip install nuitka
python -m nuitka --standalone --macos-create-app-bundle openduck.py
