home="$(dirname "$(dirname "$(readlink -fm "$0")")")"
echo $home

dist_dir="$home/dist"
rootfs="$home/docker/rootfs"

rm -rf "$dist_dir"
poetry build

rm $rootfs/*.whl
cp $home/dist/*.whl $rootfs

docker build . -t backtest
