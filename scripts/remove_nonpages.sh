# Remove all files in the dump except the pages file
# $1: Container ID of the container

sudo find /tmp/checkpoints/cont$1 -type f -not -name 'pages-*' -delete