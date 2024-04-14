./clean.sh
./logs.sh
clear && rm -f CLIENT_MANIFEST.MF && rm -f SERVER_MANIFEST.MF && ant && ant m4-test
# clear && rm -f CLIENT_MANIFEST.MF && rm -f SERVER_MANIFEST.MF && ant m4-test