rm -rf ./dist
poetry build
pip uninstall -y backtest
for file in `ls ./dist/*backtest-*.whl`;do pip -q install $file;done
